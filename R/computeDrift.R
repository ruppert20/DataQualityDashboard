# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of DataQualityDashboard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

#' Compute data drift metrics for a numeric CDM field.
#'
#' Extension of the numeric_stats analysis. Reuses the same collected Andromeda
#' frame (qData) that numeric_stats consumes, splits it by
#' (measurement_concept_id, unit_concept_id) and computes per-month distributional
#' comparisons against (a) a fixed origin baseline and (b) a rolling current
#' regime baseline. Uses bcp for probabilistic regime detection.
#'
#' Produces three CSVs alongside the existing `_stats.csv` outputs:
#'   * `<baseFilePath>_drift_monthly.csv`   - per (concept, unit, year_month)
#'   * `<baseFilePath>_drift_summary.csv`   - per (concept, unit)
#'   * `<baseFilePath>_drift_histogram.csv` - per (concept, unit, year_month, bin)
#'
#' @param qData             data frame collected from Andromeda; must contain
#'                          columns measurement_concept_id, unit_concept_id,
#'                          measurement_datetime, value_as_number.
#' @param baseFilePath      file path prefix (same convention as numeric_stats).
#' @param minMonthObs       minimum non-NA observations for a month to be
#'                          included in drift computation (default 30).
#' @param originWindowMonths months used to build the fixed origin baseline
#'                          (default 12).
#' @param nBins             number of quantile bins for PSI/JSD (default 10).
#' @param bcpThreshold      posterior probability above which a month starts
#'                          a new regime (default 0.5).
#' @param driftPsiThreshold PSI threshold for the anomaly flag (default 0.25).
#' @param minRegimeMonths   minimum duration (in months) for a bcp-detected
#'                          segment to count as a real regime. Segments
#'                          shorter than this are merged into an adjacent
#'                          regime and the affected months are flagged as
#'                          anomalies instead. Default 3.
#' @param memoryBudgetBytes numeric byte figure. When the total observation
#'                          count for a (concept, unit) group would push
#'                          memory past `budget / (8 * safetyFactor)`,
#'                          months are subsampled to fit. `Inf` (default)
#'                          disables subsampling.
#' @param safetyFactor      overhead multiplier applied to the raw
#'                          byte-per-value figure to account for dplyr
#'                          intermediate copies, Wasserstein sort buffers,
#'                          and per-row data.frame overhead. Default 5.
#'
#' @return invisibly, a list with elements `monthly`, `summary`, `histogram`.
#'
#' @importFrom rlang .data
#' @importFrom magrittr %>%
#'
#' @keywords internal
.computeDrift <- function(qData,
                          baseFilePath,
                          minMonthObs = 30,
                          originWindowMonths = 12,
                          nBins = 10,
                          bcpThreshold = 0.5,
                          driftPsiThreshold = 0.25,
                          minRegimeMonths,
                          memoryBudgetBytes = Inf,
                          safetyFactor = 5) {

  emptyOutputs <- list(
    monthly = .emptyDriftMonthly(),
    summary = .emptyDriftSummary(),
    histogram = .emptyDriftHistogram()
  )

  .writeDriftCsvs(emptyOutputs, baseFilePath)

  if (is.null(qData) || nrow(qData) == 0) {
    return(invisible(emptyOutputs))
  }

  # is.finite() rejects NA, NaN, and Inf in one go — Inf would corrupt
  # mean/sd/Wasserstein without triggering the NA filter.
  df <- qData %>%
    dplyr::filter(is.finite(.data$value_as_number),
                  !is.na(.data$measurement_datetime)) %>%
    dplyr::mutate(year_month = .toYearMonth(.data$measurement_datetime))

  if (nrow(df) == 0) {
    return(invisible(emptyOutputs))
  }

  groups <- df %>%
    dplyr::distinct(.data$measurement_concept_id, .data$unit_concept_id) %>%
    as.data.frame()

  monthly_all <- vector("list", nrow(groups))
  summary_all <- vector("list", nrow(groups))
  histogram_all <- vector("list", nrow(groups))

  for (i in seq_len(nrow(groups))) {
    cid <- groups$measurement_concept_id[i]
    uid <- groups$unit_concept_id[i]

    if (is.na(uid)) {
      grp <- df %>%
        dplyr::filter(.data$measurement_concept_id == cid,
                      is.na(.data$unit_concept_id))
    } else {
      grp <- df %>%
        dplyr::filter(.data$measurement_concept_id == cid,
                      .data$unit_concept_id == uid)
    }

    res <- tryCatch(
      .driftForGroup(grp, cid, uid,
                     minMonthObs, originWindowMonths, nBins,
                     bcpThreshold, driftPsiThreshold, minRegimeMonths,
                     memoryBudgetBytes, safetyFactor),
      error = function(e) {
        ParallelLogger::logWarn(sprintf(
          "Drift computation failed for concept=%s unit=%s: %s",
          as.character(cid), as.character(uid), conditionMessage(e)))
        NULL
      }
    )

    if (!is.null(res)) {
      monthly_all[[i]] <- res$monthly
      summary_all[[i]] <- res$summary
      histogram_all[[i]] <- res$histogram
    }
  }

  outputs <- list(
    monthly = dplyr::bind_rows(monthly_all),
    summary = dplyr::bind_rows(summary_all),
    histogram = dplyr::bind_rows(histogram_all)
  )

  .writeDriftCsvs(outputs, baseFilePath)
  invisible(outputs)
}


.driftForGroup <- function(grp, cid, uid,
                           minMonthObs, originWindowMonths, nBins,
                           bcpThreshold, driftPsiThreshold,
                           minRegimeMonths,
                           memoryBudgetBytes = Inf,
                           safetyFactor = 5) {

  if (nrow(grp) == 0) return(NULL)

  # Consistent prefix so operators can `grep '\[drift/CID]'` in a large log
  # file to trace one concept, or `grep '\[drift]'` for all drift activity.
  tag <- sprintf("[drift/%s/%s]", as.character(cid), as.character(uid))
  gc_reset <- gc(verbose = FALSE, reset = TRUE)
  t0 <- Sys.time()
  ParallelLogger::logInfo(sprintf(
    "%s entry: %d rows across candidate months (concept size before grouping)",
    tag, nrow(grp)))

  # ---- Memory-adaptive per-month subsampling ---------------------------------
  # Adaptive redistribution algorithm: rather than assign every month the
  # same cap (budget/n_months) and waste allocation on small months that
  # cannot use it, we iteratively promote "small" months out of the pool
  # (months whose count already fits under the current fair share). Their
  # unused portion is redistributed to the remaining "big" months. Result:
  # small months are never touched, and big months get the maximum sample
  # size the budget allows.
  monthly_counts <- table(grp$year_month)
  n_months <- length(monthly_counts)
  n_obs_original_by_month <- as.integer(monthly_counts)
  names(n_obs_original_by_month) <- names(monthly_counts)

  per_month_cap <- .adaptivePerMonthCap(
    counts = n_obs_original_by_month,
    memoryBudgetBytes = memoryBudgetBytes,
    safetyFactor = safetyFactor,
    minMonthObs = minMonthObs
  )
  big_month_mask <- n_obs_original_by_month > per_month_cap

  if (any(big_month_mask)) {
    big_cap <- max(per_month_cap[big_month_mask])
    ParallelLogger::logInfo(sprintf(
      paste("Drift subsampling concept=%s unit=%s: %d/%d months capped at",
            "%d obs (budget %.0f MB, safety factor %d, small months",
            "kept intact)"),
      as.character(cid), as.character(uid),
      sum(big_month_mask), n_months,
      big_cap, memoryBudgetBytes / 1024^2, safetyFactor))
    # Random subsample per month using base-R split+sample. Memory-efficient
    # (only allocates integer row indices, then subsets once at the end).
    rows_by_month <- split(seq_len(nrow(grp)), grp$year_month)
    kept_rows <- unlist(lapply(names(rows_by_month), function(m) {
      rows <- rows_by_month[[m]]
      cap <- per_month_cap[m]
      if (length(rows) <= cap) rows else sample(rows, size = cap, replace = FALSE)
    }), use.names = FALSE)
    grp <- grp[sort(kept_rows), , drop = FALSE]
  }

  ParallelLogger::logInfo(sprintf(
    "%s building monthly_raw list-column (%d rows, %d months)",
    tag, nrow(grp), n_months))
  monthly_raw <- grp %>%
    dplyr::group_by(.data$year_month) %>%
    dplyr::summarise(
      n_obs_used = dplyr::n(),
      m_mean = mean(.data$value_as_number, na.rm = TRUE),
      m_sd = stats::sd(.data$value_as_number, na.rm = TRUE),
      values = list(.data$value_as_number),
      .groups = "drop"
    ) %>%
    dplyr::arrange(.data$year_month) %>%
    dplyr::mutate(
      n_obs = as.integer(n_obs_original_by_month[.data$year_month])
    )

  if (nrow(monthly_raw) < 2) {
    ParallelLogger::logInfo(sprintf(
      "%s abort: only %d monthly rows after grouping (need >= 2)",
      tag, nrow(monthly_raw)))
    return(NULL)
  }

  # Eligibility is based on ORIGINAL n_obs — subsampling never demotes a month
  # into insufficient-data territory because the cap is floored at minMonthObs.
  eligible <- monthly_raw %>% dplyr::filter(.data$n_obs >= minMonthObs)
  if (nrow(eligible) == 0) {
    ParallelLogger::logInfo(sprintf(
      "%s abort: no month has >= %d observations (all insufficient)",
      tag, minMonthObs))
    return(NULL)
  }

  origin_months <- utils::head(eligible$year_month, originWindowMonths)
  origin_values <- unlist(
    monthly_raw$values[monthly_raw$year_month %in% origin_months],
    use.names = FALSE
  )
  if (length(origin_values) < minMonthObs) {
    ParallelLogger::logInfo(sprintf(
      "%s abort: origin baseline pool has only %d values (need >= %d)",
      tag, length(origin_values), minMonthObs))
    return(NULL)
  }

  bin_breaks <- .quantileBinBreaks(origin_values, nBins)
  if (is.null(bin_breaks)) {
    ParallelLogger::logInfo(sprintf(
      "%s abort: could not construct quantile bins from origin baseline",
      tag))
    return(NULL)
  }
  n_bins_actual <- length(bin_breaks) - 1L
  ParallelLogger::logInfo(sprintf(
    "%s origin baseline: %d months, %d values, %d bins",
    tag, length(origin_months), length(origin_values), n_bins_actual))

  origin_hist <- .valuesToHistProps(origin_values, bin_breaks)

  monthly <- monthly_raw %>%
    dplyr::mutate(
      insufficient_data = .data$n_obs < minMonthObs
    )

  monthly$psi_origin <- NA_real_
  monthly$wasserstein_origin <- NA_real_
  monthly$jsd_origin <- NA_real_
  monthly$psi_current <- NA_real_
  monthly$wasserstein_current <- NA_real_
  monthly$jsd_current <- NA_real_
  monthly$bcp_posterior <- NA_real_
  monthly$regime_id <- NA_integer_
  monthly$is_regime_start <- FALSE
  monthly$is_anomaly <- FALSE

  hist_rows <- vector("list", nrow(monthly))

  n_eligible <- sum(!monthly$insufficient_data)
  ParallelLogger::logInfo(sprintf(
    "%s origin-comparison phase: %d eligible months x [PSI, JSD, Wasserstein]",
    tag, n_eligible))
  origin_pool_size <- length(origin_values)
  phase_start <- Sys.time()

  for (m in seq_len(nrow(monthly))) {
    if (monthly$insufficient_data[m]) next

    mvals <- monthly$values[[m]]
    mvals <- mvals[!is.na(mvals)]
    if (length(mvals) == 0) next

    m_props <- .valuesToHistProps(mvals, bin_breaks)
    m_counts <- .valuesToHistCounts(mvals, bin_breaks)

    monthly$psi_origin[m] <- .psi(m_props, origin_hist)
    monthly$jsd_origin[m] <- .jsdSafe(m_props, origin_hist,
                                       ctx = tag, month = monthly$year_month[m])
    monthly$wasserstein_origin[m] <- .wassersteinSafe(
      mvals, origin_values,
      ctx = tag, month = monthly$year_month[m])

    hist_rows[[m]] <- data.frame(
      measurement_concept_id = cid,
      unit_concept_id = uid,
      year_month = monthly$year_month[m],
      bin_index = seq_len(n_bins_actual),
      bin_lower = bin_breaks[-length(bin_breaks)],
      bin_upper = bin_breaks[-1],
      count = m_counts,
      proportion = m_props,
      stringsAsFactors = FALSE
    )
  }
  ParallelLogger::logInfo(sprintf(
    "%s origin-comparison phase complete (%.1fs)",
    tag, as.numeric(difftime(Sys.time(), phase_start, units = "secs"))))

  eligible_idx <- which(!monthly$insufficient_data)
  if (length(eligible_idx) >= 2) {
    bcp_input <- cbind(
      mean = monthly$m_mean[eligible_idx],
      sd = ifelse(is.na(monthly$m_sd[eligible_idx]), 0,
                  monthly$m_sd[eligible_idx])
    )
    ParallelLogger::logInfo(sprintf(
      "%s bcp: fitting bivariate change-point model on %d months",
      tag, nrow(bcp_input)))
    bcp_start <- Sys.time()
    bcp_post <- tryCatch({
      # suppressWarnings: bcp emits a benign "built under R x.y.z" message on
      # first lazy-load; DQD's outer warning handler would otherwise abort.
      bcp_res <- suppressWarnings(bcp::bcp(bcp_input))
      as.numeric(bcp_res$posterior.prob)
    }, error = function(e) {
      ParallelLogger::logWarn(sprintf(
        "%s bcp FAILED (input %dx%d, mean range [%.4g,%.4g]): %s",
        tag, nrow(bcp_input), ncol(bcp_input),
        min(bcp_input[, "mean"]), max(bcp_input[, "mean"]),
        conditionMessage(e)))
      rep(NA_real_, length(eligible_idx))
    })
    ParallelLogger::logInfo(sprintf(
      "%s bcp complete (%.1fs, %d posteriors > 0.5)",
      tag, as.numeric(difftime(Sys.time(), bcp_start, units = "secs")),
      sum(bcp_post > bcpThreshold, na.rm = TRUE)))
    monthly$bcp_posterior[eligible_idx] <- bcp_post
  }

  # Regime assignment operates only on eligible months.
  # bcp convention (Barry & Hartigan / Erdman & Emerson 2007): posterior.prob[i] is
  # P(change occurs between position i and i+1); the last element is always NA.
  # Therefore position i is the LAST observation of the old regime and i+1 is the
  # FIRST of the new. To flag "is this month the start of a new regime", we shift
  # the posterior by one lag: is_regime_start[t] <- posterior[t-1] > threshold.
  if (length(eligible_idx) > 0) {
    posterior_here <- monthly$bcp_posterior[eligible_idx]
    is_start <- rep(FALSE, length(eligible_idx))
    is_start[1] <- TRUE
    if (length(eligible_idx) >= 2) {
      lagged <- posterior_here[-length(posterior_here)] > bcpThreshold
      lagged[is.na(lagged)] <- FALSE
      is_start[-1] <- is_start[-1] | lagged
    }
    # Merge segments shorter than minRegimeMonths into an adjacent regime.
    # bcp can be noisy — a 1-month "regime" is almost always an anomaly, not
    # a real baseline change. Merged months keep their high psi_current
    # (computed later against the merged regime baseline) and therefore get
    # flagged by the is_anomaly logic downstream.
    if (minRegimeMonths > 1 && sum(is_start) > 1) {
      starts <- which(is_start)
      starts <- .mergeShortRegimes(starts, length(is_start), minRegimeMonths)
      is_start <- logical(length(is_start))
      is_start[starts] <- TRUE
    }
    monthly$is_regime_start[eligible_idx] <- is_start
    monthly$regime_id[eligible_idx] <- cumsum(is_start)
  }

  regime_ids <- unique(stats::na.omit(monthly$regime_id))
  ParallelLogger::logInfo(sprintf(
    "%s current-regime phase: pooling values across %d regime(s)",
    tag, length(regime_ids)))
  regime_start <- Sys.time()
  for (rid in regime_ids) {
    regime_rows <- which(monthly$regime_id == rid & !is.na(monthly$regime_id))
    regime_values <- unlist(monthly$values[regime_rows], use.names = FALSE)
    regime_values <- regime_values[!is.na(regime_values)]
    if (length(regime_values) < minMonthObs) next
    regime_hist <- .valuesToHistProps(regime_values, bin_breaks)

    for (m in regime_rows) {
      mvals <- monthly$values[[m]]
      mvals <- mvals[!is.na(mvals)]
      if (length(mvals) == 0) next
      m_props <- .valuesToHistProps(mvals, bin_breaks)
      monthly$psi_current[m] <- .psi(m_props, regime_hist)
      monthly$jsd_current[m] <- .jsdSafe(m_props, regime_hist,
                                         ctx = tag,
                                         month = monthly$year_month[m])
      monthly$wasserstein_current[m] <- .wassersteinSafe(
        mvals, regime_values,
        ctx = tag, month = monthly$year_month[m])
    }
  }
  ParallelLogger::logInfo(sprintf(
    "%s current-regime phase complete (%.1fs)",
    tag, as.numeric(difftime(Sys.time(), regime_start, units = "secs"))))

  monthly$is_anomaly <- !is.na(monthly$psi_current) &
    monthly$psi_current > driftPsiThreshold &
    !monthly$is_regime_start

  # Regime length: number of eligible months in each regime, joined back onto
  # every eligible month. Ineligible months (insufficient data) get NA.
  regime_lengths <- monthly %>%
    dplyr::filter(!is.na(.data$regime_id)) %>%
    dplyr::count(.data$regime_id, name = "regime_length_months")
  monthly <- monthly %>%
    dplyr::left_join(regime_lengths, by = "regime_id")

  monthly_out <- monthly %>%
    dplyr::mutate(
      measurement_concept_id = cid,
      unit_concept_id = uid
    ) %>%
    dplyr::select(
      "measurement_concept_id", "unit_concept_id", "year_month",
      "n_obs", "n_obs_used",
      "psi_origin", "wasserstein_origin", "jsd_origin",
      "psi_current", "wasserstein_current", "jsd_current",
      "bcp_posterior", "regime_id", "regime_length_months",
      "is_regime_start", "is_anomaly", "insufficient_data"
    )

  regime_starts <- monthly_out$year_month[monthly_out$is_regime_start]
  regime_start_lengths <- monthly_out$regime_length_months[monthly_out$is_regime_start]
  anomalous <- monthly_out$year_month[monthly_out$is_anomaly]
  n_regimes <- length(regime_starts)
  current_regime_start <- if (n_regimes > 0) utils::tail(regime_starts, 1) else NA_character_
  current_regime_length <- if (n_regimes > 0) utils::tail(regime_start_lengths, 1) else NA_integer_

  summary_out <- data.frame(
    measurement_concept_id = cid,
    unit_concept_id = uid,
    origin_baseline_start = utils::head(origin_months, 1),
    origin_baseline_end = utils::tail(origin_months, 1),
    origin_n_months = length(origin_months),
    origin_n_obs = length(origin_values),
    n_bins = n_bins_actual,
    n_regimes = n_regimes,
    regime_change_months = paste(regime_starts, collapse = ";"),
    regime_lengths_months = paste(regime_start_lengths, collapse = ";"),
    current_regime_start = current_regime_start,
    current_regime_length_months = current_regime_length,
    n_anomalous_months = length(anomalous),
    anomalous_months = paste(anomalous, collapse = ";"),
    max_psi_origin = suppressWarnings(max(monthly_out$psi_origin, na.rm = TRUE)),
    max_psi_current = suppressWarnings(max(monthly_out$psi_current, na.rm = TRUE)),
    max_wasserstein_origin = suppressWarnings(max(monthly_out$wasserstein_origin, na.rm = TRUE)),
    max_wasserstein_current = suppressWarnings(max(monthly_out$wasserstein_current, na.rm = TRUE)),
    stringsAsFactors = FALSE
  )
  # suppressWarnings(max(NA_real_)) returns -Inf; normalize to NA.
  for (col in c("max_psi_origin", "max_psi_current",
                "max_wasserstein_origin", "max_wasserstein_current")) {
    if (is.infinite(summary_out[[col]])) summary_out[[col]] <- NA_real_
  }

  histogram_out <- dplyr::bind_rows(hist_rows)

  # ---- Pattern-classification features ---------------------------------------
  # Per-regime aggregate stats: mean, sd, p5, p95 over the regime's pooled values.
  regime_stats_list <- lapply(sort(unique(stats::na.omit(monthly$regime_id))),
                              function(rid) {
    rr <- which(monthly$regime_id == rid & !is.na(monthly$regime_id))
    v <- unlist(monthly$values[rr], use.names = FALSE)
    v <- v[!is.na(v)]
    if (length(v) == 0) return(NULL)
    data.frame(
      regime_id = rid,
      regime_mean = mean(v),
      regime_sd = stats::sd(v),
      regime_p5 = as.numeric(stats::quantile(v, 0.05, names = FALSE)),
      regime_p95 = as.numeric(stats::quantile(v, 0.95, names = FALSE)),
      regime_n = length(v)
    )
  })
  regime_stats <- dplyr::bind_rows(regime_stats_list)

  # First-vs-last regime deltas as % change (drives location/scale/tail patterns).
  if (nrow(regime_stats) >= 2) {
    fr <- regime_stats[1, ]
    lr <- regime_stats[nrow(regime_stats), ]
    delta_mean_pct <- .pctChange(fr$regime_mean, lr$regime_mean)
    delta_sd_pct <- .pctChange(fr$regime_sd, lr$regime_sd)
    delta_p5_pct <- .pctChange(fr$regime_p5, lr$regime_p5)
    delta_p95_pct <- .pctChange(fr$regime_p95, lr$regime_p95)
  } else {
    delta_mean_pct <- delta_sd_pct <- delta_p5_pct <- delta_p95_pct <- NA_real_
  }
  min_regime_length_months <- if (length(regime_start_lengths) > 0)
    min(regime_start_lengths, na.rm = TRUE) else NA_integer_
  max_regime_length_months <- if (length(regime_start_lengths) > 0)
    max(regime_start_lengths, na.rm = TRUE) else NA_integer_

  # Trend tests: Mann-Kendall on monthly mean AND on monthly psi_origin.
  ParallelLogger::logInfo(sprintf(
    "%s trend + seasonality + dip diagnostics", tag))
  diag_start <- Sys.time()
  eligible_monthly <- monthly_out %>%
    dplyr::filter(!.data$insufficient_data) %>%
    dplyr::arrange(.data$year_month)
  # month means come from monthly (with values); align eligible ones:
  eligible_means <- monthly$m_mean[!monthly$insufficient_data]
  tr_mean <- .trendTest(eligible_means)
  tr_psi <- .trendTest(eligible_monthly$psi_origin)

  # Seasonality: ACF at lag 12 on monthly mean.
  seas <- .seasonalityLag12(eligible_means)

  # Bimodality: dip test on origin baseline vs. current-regime pooled values.
  current_regime_values <- if (nrow(regime_stats) >= 1) {
    rr <- which(monthly$regime_id == utils::tail(regime_stats$regime_id, 1))
    v <- unlist(monthly$values[rr], use.names = FALSE)
    v[!is.na(v)]
  } else numeric(0)
  dip_orig <- .dipTest(origin_values, ctx = tag, which = "origin")
  dip_curr <- .dipTest(current_regime_values,
                        ctx = tag, which = "current-regime")
  ParallelLogger::logInfo(sprintf(
    "%s diagnostics complete (%.1fs)",
    tag, as.numeric(difftime(Sys.time(), diag_start, units = "secs"))))

  # Priority: sortable "how much drift × how much data" score.
  psi_max <- suppressWarnings(max(c(summary_out$max_psi_origin,
                                    summary_out$max_psi_current), na.rm = TRUE))
  if (!is.finite(psi_max)) psi_max <- 0
  total_n_obs <- sum(monthly$n_obs, na.rm = TRUE)
  priority_score <- psi_max * log10(1 + total_n_obs)

  # Classifier.
  features <- list(
    max_psi_origin = summary_out$max_psi_origin,
    max_psi_current = summary_out$max_psi_current,
    n_regimes = summary_out$n_regimes,
    min_regime_length_months = min_regime_length_months,
    max_regime_length_months = max_regime_length_months,
    n_anomalous_months = length(anomalous),
    delta_mean_pct = delta_mean_pct,
    delta_sd_pct = delta_sd_pct,
    delta_p5_pct = delta_p5_pct,
    delta_p95_pct = delta_p95_pct,
    trend_tau_mean = tr_mean$tau,
    trend_pvalue_mean = tr_mean$p_value,
    seasonality_significant = seas$significant,
    dip_pvalue_origin = dip_orig$p_value,
    dip_pvalue_current = dip_curr$p_value,
    driftPsiThreshold = driftPsiThreshold
  )
  cls <- .classifyPattern(features)

  # Extend summary with classification + tests + priority.
  summary_out$delta_mean_pct <- delta_mean_pct
  summary_out$delta_sd_pct <- delta_sd_pct
  summary_out$delta_p5_pct <- delta_p5_pct
  summary_out$delta_p95_pct <- delta_p95_pct
  summary_out$min_regime_length_months <- min_regime_length_months
  summary_out$max_regime_length_months <- max_regime_length_months
  summary_out$trend_tau_mean <- tr_mean$tau
  summary_out$trend_pvalue_mean <- tr_mean$p_value
  summary_out$trend_tau_psi <- tr_psi$tau
  summary_out$trend_pvalue_psi <- tr_psi$p_value
  summary_out$seasonality_acf_lag12 <- seas$acf_lag12
  summary_out$seasonality_significant <- seas$significant
  summary_out$dip_stat_origin <- dip_orig$dip
  summary_out$dip_pvalue_origin <- dip_orig$p_value
  summary_out$dip_stat_current <- dip_curr$dip
  summary_out$dip_pvalue_current <- dip_curr$p_value
  summary_out$priority_score <- priority_score
  summary_out$pattern_type <- cls$pattern_type
  summary_out$pattern_tags <- cls$pattern_tags

  # Subsampling annotations (per user request): report how much of the raw
  # data actually fed the drift calculation. If nothing was subsampled,
  # n_obs_used == n_obs and pct_obs_used == 100.
  total_obs_original <- sum(monthly_out$n_obs, na.rm = TRUE)
  total_obs_used <- sum(monthly_out$n_obs_used, na.rm = TRUE)
  summary_out$total_obs_original <- as.integer(total_obs_original)
  summary_out$total_obs_used <- as.integer(total_obs_used)
  summary_out$total_obs_excluded <- as.integer(total_obs_original - total_obs_used)
  summary_out$pct_obs_used <- if (total_obs_original > 0)
    round(100 * total_obs_used / total_obs_original, 4) else NA_real_
  summary_out$subsampled_any <- any(monthly_out$n_obs_used < monthly_out$n_obs,
                                    na.rm = TRUE)
  summary_out$memory_budget_mb <- if (is.finite(memoryBudgetBytes))
    round(memoryBudgetBytes / 1024^2, 2) else NA_real_
  # subsample_cap_big_months: the cap applied to any month that was subsampled
  # (all big months share the same cap under adaptive redistribution). NA if
  # subsampling was disabled or no month was big enough to be capped.
  big_diff <- monthly_out$n_obs > monthly_out$n_obs_used
  summary_out$n_months_subsampled <- as.integer(sum(big_diff, na.rm = TRUE))
  summary_out$subsample_cap_big_months <- if (any(big_diff, na.rm = TRUE))
    as.integer(max(monthly_out$n_obs_used[big_diff])) else NA_integer_

  # gc() returns a 2-row matrix (Ncells / Vcells) with 7 numeric columns:
  #   [,2] = used MB,  [,7] = max-used MB since last reset.
  # Sum across the two rows to get total R heap usage.
  gc_final <- gc(verbose = FALSE)
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  ParallelLogger::logInfo(sprintf(
    paste("%s complete: %.1fs, %d regimes, %d anomalies, R memory now",
          "%.0f MB (peak this concept %.0f MB), subsampled=%s (%.1f%% of obs used)"),
    tag, elapsed,
    summary_out$n_regimes, summary_out$n_anomalous_months,
    sum(gc_final[, 2]), sum(gc_final[, 7]),
    summary_out$subsampled_any, summary_out$pct_obs_used))

  list(
    monthly = monthly_out,
    summary = summary_out,
    histogram = histogram_out
  )
}


.quantileBinBreaks <- function(x, nBins) {
  x <- x[!is.na(x)]
  if (length(x) < nBins) return(NULL)
  interior_probs <- seq(1 / nBins, 1 - 1 / nBins, by = 1 / nBins)
  interior <- unique(as.numeric(stats::quantile(x, probs = interior_probs, na.rm = TRUE)))
  if (length(interior) == 0) return(NULL)
  c(-Inf, interior, Inf)
}


.valuesToHistCounts <- function(values, breaks) {
  bin_idx <- cut(values, breaks = breaks, include.lowest = TRUE, right = TRUE, labels = FALSE)
  tabulate(bin_idx, nbins = length(breaks) - 1L)
}


.valuesToHistProps <- function(values, breaks) {
  counts <- .valuesToHistCounts(values, breaks)
  total <- sum(counts)
  if (total == 0) return(rep(0, length(counts)))
  counts / total
}


.psi <- function(actual, expected, epsilon = 1e-6) {
  a <- pmax(actual, epsilon)
  e <- pmax(expected, epsilon)
  sum((a - e) * log(a / e))
}


# All three wrappers take optional ctx/month/which context so that on a native
# crash or R error we log WHICH concept + WHICH month + WHICH call point died,
# with the input sizes. Without this a segfault in transport::wasserstein1d
# gives R "session terminated" and nothing else — no way to know which specific
# comparison killed things.
.jsdSafe <- function(p, q, ctx = NULL, month = NULL) {
  tryCatch({
    val <- suppressMessages(
      philentropy::JSD(rbind(p, q), unit = "log2", est.prob = NULL, test.na = FALSE)
    )
    as.numeric(val)
  }, error = function(e) {
    if (!is.null(ctx)) {
      ParallelLogger::logWarn(sprintf(
        "%s JSD failed at %s (p len=%d, q len=%d): %s",
        ctx, month %||% "?", length(p), length(q), conditionMessage(e)))
    }
    NA_real_
  })
}


.wassersteinSafe <- function(a, b, ctx = NULL, month = NULL) {
  tryCatch({
    as.numeric(transport::wasserstein1d(a, b, p = 1))
  }, error = function(e) {
    if (!is.null(ctx)) {
      ParallelLogger::logWarn(sprintf(
        "%s Wasserstein failed at %s (a len=%d, b len=%d): %s",
        ctx, month %||% "?", length(a), length(b), conditionMessage(e)))
    }
    NA_real_
  })
}


`%||%` <- function(a, b) if (is.null(a) || (length(a) == 1 && is.na(a))) b else a


# Timezone-safe YYYY-MM extraction: preserves whichever TZ the source column
# carries (or UTC if none), avoiding silent day/month shifts when the caller's
# system TZ differs from the CDM's stored TZ. as.Date() on POSIXct would use
# Sys.timezone() by default which is the source of the bug.
.toYearMonth <- function(x) {
  if (inherits(x, "POSIXt")) {
    tz <- attr(x, "tzone")
    if (is.null(tz) || length(tz) == 0 || tz == "") tz <- "UTC"
    return(format(x, "%Y-%m", tz = tz))
  }
  if (inherits(x, "Date")) return(format(x, "%Y-%m"))
  # Fallback: coerce via UTC to avoid the system-tz trap.
  format(as.POSIXct(as.character(x), tz = "UTC"), "%Y-%m", tz = "UTC")
}


# Cross-platform "how much RAM can I use right now" probe.
# Linux reads /proc/meminfo (MemAvailable is what the kernel thinks apps can
# claim without swapping); macOS parses vm_stat's free + inactive pages;
# Windows queries FreePhysicalMemory via wmic. Returns NA on any failure,
# and the caller falls back to no-subsampling in that case.
.availableMemoryBytes <- function() {
  sysname <- unname(Sys.info()["sysname"])
  bytes <- NA_real_
  tryCatch({
    if (sysname == "Linux") {
      meminfo <- readLines("/proc/meminfo", warn = FALSE)
      line <- grep("^MemAvailable:", meminfo, value = TRUE)
      if (length(line) > 0) {
        kb <- as.numeric(regmatches(line, regexpr("[0-9]+", line)))
        bytes <- kb * 1024
      }
    } else if (sysname == "Darwin") {
      out <- suppressWarnings(system("vm_stat", intern = TRUE,
                                      ignore.stderr = TRUE))
      page_line <- grep("page size of", out, value = TRUE)
      free_line <- grep("^Pages free:", out, value = TRUE)
      inactive_line <- grep("^Pages inactive:", out, value = TRUE)
      if (length(page_line) && length(free_line)) {
        page_size <- as.numeric(regmatches(page_line,
                                           regexpr("[0-9]+", page_line)))
        get_pages <- function(l) {
          v <- regmatches(l, regexpr("[0-9]+", l))
          if (length(v) == 0) 0 else as.numeric(v)
        }
        bytes <- (get_pages(free_line) + get_pages(inactive_line)) * page_size
      }
    } else if (sysname == "Windows") {
      out <- suppressWarnings(system(
        "wmic OS get FreePhysicalMemory /value",
        intern = TRUE, ignore.stderr = TRUE))
      line <- grep("FreePhysicalMemory=", out, value = TRUE)
      if (length(line) > 0) {
        kb <- as.numeric(sub("FreePhysicalMemory=", "", line))
        bytes <- kb * 1024
      }
    }
  }, error = function(e) invisible(NULL))
  bytes
}


# Emit a reproducibility banner to the DQD log at run start: DQD version,
# R version, platform, available memory, and every non-sensitive input
# parameter of executeDqChecks(). connectionDetails is explicitly excluded
# (may hold server/user/password); only its $dbms field is logged since that
# is the DB dialect used for SqlRender translation and does not identify a
# specific server. If the caller wants full redaction, they can pass
# connectionDetails = NULL after their own diagnostics.
.logRunConfig <- function(params, connectionDetails,
                          driftMemoryBudgetBytes, availableBytes) {
  ParallelLogger::logInfo(
    "======= DataQualityDashboard run configuration =======")
  ParallelLogger::logInfo(sprintf(
    "DQD version: %s",
    tryCatch(as.character(utils::packageVersion("DataQualityDashboard")),
             error = function(e) "unknown")))
  ParallelLogger::logInfo(sprintf(
    "R: %s.%s (%s)",
    R.Version()$major, R.Version()$minor, R.Version()$platform))
  si <- Sys.info()
  ParallelLogger::logInfo(sprintf(
    "Platform: %s %s (%s)",
    si["sysname"], si["release"], si["machine"]))
  ParallelLogger::logInfo(sprintf(
    "Run time: %s (%s)",
    format(Sys.time(), "%Y-%m-%d %H:%M:%S"), Sys.timezone()))

  if (is.na(availableBytes)) {
    ParallelLogger::logInfo(
      "Available memory: could not detect (subsampling will be disabled)")
  } else {
    ParallelLogger::logInfo(sprintf(
      "Available memory at start: %.2f GB",
      availableBytes / 1024^3))
  }
  ParallelLogger::logInfo(sprintf(
    "driftMemoryBudgetBytes (resolved): %s",
    if (is.infinite(driftMemoryBudgetBytes)) "Inf (subsampling disabled)"
    else sprintf("%.2f MB per worker", driftMemoryBudgetBytes / 1024^2)))
  ParallelLogger::logInfo(sprintf(
    "connectionDetails$dbms: %s",
    tryCatch(as.character(connectionDetails$dbms),
             error = function(e) "unknown")))

  ParallelLogger::logInfo("--- input parameters ---")
  # Excluded: connectionDetails (may hold secrets).
  for (nm in sort(names(params))) {
    val <- params[[nm]]
    if (is.null(val)) {
      s <- "NULL"
    } else if (is.function(val)) {
      s <- "<function>"
    } else if (length(val) == 0) {
      s <- sprintf("%s()", class(val)[1])
    } else if (length(val) == 1) {
      s <- format(val)
    } else if (length(val) <= 20) {
      s <- paste(format(val), collapse = ", ")
    } else {
      s <- sprintf("%s[1:%d]", class(val)[1], length(val))
    }
    ParallelLogger::logInfo(sprintf("  %s = %s", nm, s))
  }
  ParallelLogger::logInfo(
    "======================================================")
}


# Resolve a user-supplied budget spec ("auto" | numeric MB | Inf) into a byte
# figure for a single worker. "auto" queries the OS and takes `share` (default
# 0.25 = 25%) of what's currently available, then divides by numThreads so
# parallel workers don't collectively exceed the budget. Returns Inf if the
# probe fails, which disables subsampling.
.resolveMemoryBudgetBytes <- function(spec, numThreads = 1L, share = 0.25) {
  if (is.character(spec) && length(spec) == 1 && spec == "auto") {
    avail <- .availableMemoryBytes()
    if (is.na(avail)) return(Inf)
    return(avail * share / max(numThreads, 1L))
  }
  if (is.numeric(spec) && length(spec) == 1 && !is.na(spec)) {
    if (is.infinite(spec)) return(Inf)
    return(spec * 1024 * 1024 / max(numThreads, 1L))
  }
  Inf
}


# Adaptive per-month cap allocation. Given a named vector of monthly
# observation counts and a total-value budget, iteratively promote months
# already below the current "fair share" out of the shared pool and
# redistribute their unused budget to the remaining big months. Returns a
# named integer vector of caps (same order as `counts`); a month's kept
# sample size will be `min(counts[m], cap[m])`. Small months' caps equal
# their own count (i.e. never subsampled). Big months share the remainder
# equally. If `memoryBudgetBytes` is Inf, every cap is `.Machine$integer.max`.
.adaptivePerMonthCap <- function(counts, memoryBudgetBytes,
                                 safetyFactor, minMonthObs) {
  n_m <- length(counts)
  if (n_m == 0) return(integer(0))
  if (!is.finite(memoryBudgetBytes)) {
    out <- rep(.Machine$integer.max, n_m)
    names(out) <- names(counts)
    return(out)
  }
  allowed_total <- floor(memoryBudgetBytes / (8 * safetyFactor))
  is_small <- logical(n_m)
  remaining <- allowed_total
  repeat {
    n_big <- n_m - sum(is_small)
    if (n_big == 0) break
    fair_share <- remaining / n_big
    new_small <- !is_small & (counts <= fair_share)
    if (!any(new_small)) break
    remaining <- remaining - sum(counts[new_small])
    is_small <- is_small | new_small
  }
  n_big <- n_m - sum(is_small)
  big_cap <- if (n_big > 0) max(minMonthObs, floor(remaining / n_big))
             else .Machine$integer.max
  caps <- as.integer(counts)         # small months: cap = own count (untouched)
  caps[!is_small] <- big_cap         # big months: shared cap
  names(caps) <- names(counts)
  caps
}


# Iteratively drop regime-start boundaries whose following segment is shorter
# than min_length. When the first segment is too short, we merge it into the
# second (drop the boundary AFTER position 1) since position 1 is always the
# start of the series. All other short segments merge into the previous regime
# (drop their own boundary).
.mergeShortRegimes <- function(starts, n_positions, min_length) {
  if (length(starts) <= 1) return(starts)
  repeat {
    lens <- diff(c(starts, n_positions + 1L))
    short <- which(lens < min_length)
    if (length(short) == 0) return(starts)
    if (length(starts) == 1) return(starts)   # nothing left to merge into
    i <- short[1]
    if (i == 1L) {
      starts <- starts[-2]                    # merge regime 1 forward into 2
    } else {
      starts <- starts[-i]                    # merge regime i backward into i-1
    }
  }
}


.pctChange <- function(from_val, to_val) {
  if (is.na(from_val) || is.na(to_val)) return(NA_real_)
  if (abs(from_val) < 1e-9) return(NA_real_)
  (to_val - from_val) / abs(from_val) * 100
}


.trendTest <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) < 4) return(list(tau = NA_real_, p_value = NA_real_))
  # Guard against constant input — Kendall::MannKendall's C code emits an
  # "IFAULT 12" message to stderr when all ranks are tied (tau undefined).
  # A constant series is trivially not a trend.
  if (length(unique(x)) < 2) return(list(tau = 0, p_value = 1))
  tryCatch({
    r <- Kendall::MannKendall(x)
    list(tau = as.numeric(r$tau), p_value = as.numeric(r$sl))
  }, error = function(e) list(tau = NA_real_, p_value = NA_real_))
}


.seasonalityLag12 <- function(x) {
  x <- x[!is.na(x)]
  # Need >= 26 points to evaluate lags 11-13 reliably.
  if (length(x) < 26) {
    return(list(acf_lag12 = NA_real_, threshold = NA_real_, significant = FALSE))
  }
  tryCatch({
    a <- stats::acf(x, lag.max = 13, plot = FALSE)
    v11 <- as.numeric(a$acf[12])  # lag 11
    v12 <- as.numeric(a$acf[13])  # lag 12
    v13 <- as.numeric(a$acf[14])  # lag 13
    thr <- 2 / sqrt(length(x))
    # Require:
    #  (a) lag-12 ACF positive (annual cycle → this month correlates with
    #      same month last year), and
    #  (b) above the 95% white-noise threshold, and
    #  (c) a LOCAL PEAK at lag 12 (higher than lags 11 and 13). Trends have
    #      high ACF at every lag with slow decay, so they fail this test.
    is_seasonal <- !is.na(v12) && v12 > thr &&
                   (is.na(v11) || v12 > v11) &&
                   (is.na(v13) || v12 > v13)
    list(acf_lag12 = v12, threshold = thr, significant = is_seasonal)
  }, error = function(e) list(acf_lag12 = NA_real_,
                              threshold = NA_real_, significant = FALSE))
}


.dipTest <- function(x, ctx = NULL, which = NULL) {
  x <- x[!is.na(x)]
  if (length(x) < 20) {
    return(list(dip = NA_real_, p_value = NA_real_, is_bimodal_at_05 = NA))
  }
  tryCatch({
    # suppressMessages silences the asymptotic-approximation notice diptest
    # emits when n exceeds its built-in Monte-Carlo table (~72000).
    r <- suppressMessages(diptest::dip.test(x))
    list(dip = as.numeric(r$statistic),
         p_value = as.numeric(r$p.value),
         is_bimodal_at_05 = r$p.value < 0.05)
  }, error = function(e) {
    if (!is.null(ctx)) {
      ParallelLogger::logWarn(sprintf(
        "%s dip test failed for %s baseline (n=%d): %s",
        ctx, which %||% "?", length(x), conditionMessage(e)))
    }
    list(dip = NA_real_, p_value = NA_real_, is_bimodal_at_05 = NA)
  })
}


#' Rule-based drift pattern classifier.
#'
#' Given a set of numeric features summarizing a concept's drift behavior,
#' returns a primary `pattern_type` (single string) plus `pattern_tags`
#' (semicolon-joined labels of all matching rules). Precedence for the
#' primary label, in order:
#' `stable` (short-circuit) > `bimodal_change` > `seasonal` >
#' `transient_anomalies` > `scale_shift` > `tail_shift` > `location_shift` >
#' `monotonic_trend` > `step_change` > `gradual_drift` (fallback).
#' Distributional patterns win over temporal ones because they specify the
#' shape of the shift, which drives visualization choice in the dashboard.
#'
#' @param f  Named list of numeric features from `.driftForGroup()` —
#'           max_psi_origin, max_psi_current, driftPsiThreshold, n_regimes,
#'           min/max_regime_length_months, n_anomalous_months,
#'           delta_mean/sd/p5/p95_pct, trend_tau_mean, trend_pvalue_mean,
#'           seasonality_significant, dip_pvalue_origin, dip_pvalue_current.
#'
#' @return A list with `pattern_type` (character, one label) and
#'           `pattern_tags` (character, all matching labels joined by `;`).
#'
#' @keywords internal
.classifyPattern <- function(f) {
  psi_max <- suppressWarnings(max(c(f$max_psi_origin, f$max_psi_current),
                                  na.rm = TRUE))
  if (!is.finite(psi_max)) psi_max <- 0

  # Stable short-circuit: essentially no drift and no regimes.
  if (psi_max < 0.1 && (is.na(f$n_regimes) || f$n_regimes <= 1)) {
    return(list(pattern_type = "stable", pattern_tags = "stable"))
  }

  tags <- character(0)

  # --- Distinctive patterns first --------------------------------------------
  bimodal_flip <- !is.na(f$dip_pvalue_origin) && !is.na(f$dip_pvalue_current) &&
    ((f$dip_pvalue_origin >= 0.05 && f$dip_pvalue_current < 0.05) ||
     (f$dip_pvalue_origin < 0.05 && f$dip_pvalue_current >= 0.05))
  if (bimodal_flip) tags <- c(tags, "bimodal_change")

  if (isTRUE(f$seasonality_significant)) tags <- c(tags, "seasonal")

  # A transient is a within-regime blip severe enough that its month PSI
  # against the surrounding regime baseline is at least 2x the anomaly
  # threshold. The 2x floor excludes noise-driven borderline anomalies
  # (psi_current just barely > threshold) that a clean step change may
  # produce incidentally.
  is_transient <- !is.na(f$n_anomalous_months) && f$n_anomalous_months >= 1 &&
                  !is.na(f$max_psi_current) &&
                  f$max_psi_current > 2 * f$driftPsiThreshold &&
                  !is.na(f$max_regime_length_months) &&
                  f$max_regime_length_months >= 6
  if (is_transient) tags <- c(tags, "transient_anomalies")

  # --- Distributional shape (gated on 2-regime step, so "first vs. last"
  #     comparison is meaningful) ---------------------------------------------
  dm <- if (is.na(f$delta_mean_pct)) 0 else abs(f$delta_mean_pct)
  ds <- if (is.na(f$delta_sd_pct)) 0 else abs(f$delta_sd_pct)
  dp5 <- if (is.na(f$delta_p5_pct)) 0 else abs(f$delta_p5_pct)
  dp95 <- if (is.na(f$delta_p95_pct)) 0 else abs(f$delta_p95_pct)

  two_regime_step <- !is.na(f$n_regimes) && f$n_regimes == 2 &&
                     !is.na(f$min_regime_length_months) &&
                     f$min_regime_length_months >= 3

  if (two_regime_step && dm > 10 && ds < 20) tags <- c(tags, "location_shift")
  if (two_regime_step && ds > 20 && dm < 10) tags <- c(tags, "scale_shift")
  if (two_regime_step && (dp5 > 15 || dp95 > 15) && dm < 5 && ds < 15)
    tags <- c(tags, "tail_shift")

  # --- Temporal shape (fall through if no distributional pattern matched) ---
  # Trend uses a stricter |tau| > 0.7 to avoid firing on step functions
  # (Kendall's tau on a two-regime step is ~0.5).
  is_trend <- !is.na(f$trend_pvalue_mean) &&
              f$trend_pvalue_mean < 0.05 &&
              !is.na(f$trend_tau_mean) &&
              abs(f$trend_tau_mean) > 0.7
  if (is_trend) tags <- c(tags, "monotonic_trend")

  if (two_regime_step) tags <- c(tags, "step_change")

  # Precedence: distributional patterns first (they specify the KIND of shift
  # → drives graph choice), then distinctive temporal patterns, then generic
  # temporal signals, then fallback.
  precedence <- c("bimodal_change",
                  "seasonal",
                  "transient_anomalies",
                  "scale_shift",
                  "tail_shift",
                  "location_shift",
                  "monotonic_trend",
                  "step_change")
  primary <- NA_character_
  for (p in precedence) {
    if (p %in% tags) { primary <- p; break }
  }
  if (is.na(primary)) primary <- "gradual_drift"

  list(pattern_type = primary,
       pattern_tags = paste(tags, collapse = ";"))
}


.writeDriftCsvs <- function(outputs, baseFilePath) {
  utils::write.csv(outputs$monthly,
                   paste(baseFilePath, "drift_monthly.csv", sep = "_"),
                   row.names = FALSE)
  utils::write.csv(outputs$summary,
                   paste(baseFilePath, "drift_summary.csv", sep = "_"),
                   row.names = FALSE)
  utils::write.csv(outputs$histogram,
                   paste(baseFilePath, "drift_histogram.csv", sep = "_"),
                   row.names = FALSE)
}


.emptyDriftMonthly <- function() {
  data.frame(
    measurement_concept_id = character(0),
    unit_concept_id = character(0),
    year_month = character(0),
    n_obs = integer(0),
    n_obs_used = integer(0),
    psi_origin = numeric(0),
    wasserstein_origin = numeric(0),
    jsd_origin = numeric(0),
    psi_current = numeric(0),
    wasserstein_current = numeric(0),
    jsd_current = numeric(0),
    bcp_posterior = numeric(0),
    regime_id = integer(0),
    regime_length_months = integer(0),
    is_regime_start = logical(0),
    is_anomaly = logical(0),
    insufficient_data = logical(0),
    stringsAsFactors = FALSE
  )
}


.emptyDriftSummary <- function() {
  data.frame(
    measurement_concept_id = character(0),
    unit_concept_id = character(0),
    origin_baseline_start = character(0),
    origin_baseline_end = character(0),
    origin_n_months = integer(0),
    origin_n_obs = integer(0),
    n_bins = integer(0),
    n_regimes = integer(0),
    regime_change_months = character(0),
    regime_lengths_months = character(0),
    current_regime_start = character(0),
    current_regime_length_months = integer(0),
    n_anomalous_months = integer(0),
    anomalous_months = character(0),
    max_psi_origin = numeric(0),
    max_psi_current = numeric(0),
    max_wasserstein_origin = numeric(0),
    max_wasserstein_current = numeric(0),
    delta_mean_pct = numeric(0),
    delta_sd_pct = numeric(0),
    delta_p5_pct = numeric(0),
    delta_p95_pct = numeric(0),
    min_regime_length_months = integer(0),
    max_regime_length_months = integer(0),
    trend_tau_mean = numeric(0),
    trend_pvalue_mean = numeric(0),
    trend_tau_psi = numeric(0),
    trend_pvalue_psi = numeric(0),
    seasonality_acf_lag12 = numeric(0),
    seasonality_significant = logical(0),
    dip_stat_origin = numeric(0),
    dip_pvalue_origin = numeric(0),
    dip_stat_current = numeric(0),
    dip_pvalue_current = numeric(0),
    priority_score = numeric(0),
    pattern_type = character(0),
    pattern_tags = character(0),
    total_obs_original = integer(0),
    total_obs_used = integer(0),
    total_obs_excluded = integer(0),
    pct_obs_used = numeric(0),
    subsampled_any = logical(0),
    memory_budget_mb = numeric(0),
    n_months_subsampled = integer(0),
    subsample_cap_big_months = integer(0),
    stringsAsFactors = FALSE
  )
}


.emptyDriftHistogram <- function() {
  data.frame(
    measurement_concept_id = character(0),
    unit_concept_id = character(0),
    year_month = character(0),
    bin_index = integer(0),
    bin_lower = numeric(0),
    bin_upper = numeric(0),
    count = integer(0),
    proportion = numeric(0),
    stringsAsFactors = FALSE
  )
}
