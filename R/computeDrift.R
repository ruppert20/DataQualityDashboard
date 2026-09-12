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
#'
#' @return invisibly, a list with elements `monthly`, `summary`, `histogram`.
#'
#' @keywords internal
.computeDrift <- function(qData,
                          baseFilePath,
                          minMonthObs = 30,
                          originWindowMonths = 12,
                          nBins = 10,
                          bcpThreshold = 0.5,
                          driftPsiThreshold = 0.25) {

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
    dplyr::filter(is.finite(value_as_number), !is.na(measurement_datetime)) %>%
    dplyr::mutate(year_month = .toYearMonth(measurement_datetime))

  if (nrow(df) == 0) {
    return(invisible(emptyOutputs))
  }

  groups <- df %>%
    dplyr::distinct(measurement_concept_id, unit_concept_id) %>%
    as.data.frame()

  monthly_all <- vector("list", nrow(groups))
  summary_all <- vector("list", nrow(groups))
  histogram_all <- vector("list", nrow(groups))

  for (i in seq_len(nrow(groups))) {
    cid <- groups$measurement_concept_id[i]
    uid <- groups$unit_concept_id[i]

    if (is.na(uid)) {
      grp <- df %>%
        dplyr::filter(measurement_concept_id == cid, is.na(unit_concept_id))
    } else {
      grp <- df %>%
        dplyr::filter(measurement_concept_id == cid, unit_concept_id == uid)
    }

    res <- tryCatch(
      .driftForGroup(grp, cid, uid,
                     minMonthObs, originWindowMonths, nBins,
                     bcpThreshold, driftPsiThreshold),
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
                           bcpThreshold, driftPsiThreshold) {

  if (nrow(grp) == 0) return(NULL)

  monthly_raw <- grp %>%
    dplyr::group_by(year_month) %>%
    dplyr::summarise(
      n_obs = dplyr::n(),
      m_mean = mean(value_as_number, na.rm = TRUE),
      m_sd = stats::sd(value_as_number, na.rm = TRUE),
      values = list(value_as_number),
      .groups = "drop"
    ) %>%
    dplyr::arrange(year_month)

  if (nrow(monthly_raw) < 2) return(NULL)

  eligible <- monthly_raw %>% dplyr::filter(n_obs >= minMonthObs)
  if (nrow(eligible) == 0) return(NULL)

  origin_months <- utils::head(eligible$year_month, originWindowMonths)
  origin_values <- unlist(
    monthly_raw$values[monthly_raw$year_month %in% origin_months],
    use.names = FALSE
  )
  if (length(origin_values) < minMonthObs) return(NULL)

  bin_breaks <- .quantileBinBreaks(origin_values, nBins)
  if (is.null(bin_breaks)) return(NULL)
  n_bins_actual <- length(bin_breaks) - 1L

  origin_hist <- .valuesToHistProps(origin_values, bin_breaks)

  monthly <- monthly_raw %>%
    dplyr::mutate(
      insufficient_data = n_obs < minMonthObs
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

  for (m in seq_len(nrow(monthly))) {
    if (monthly$insufficient_data[m]) next

    mvals <- monthly$values[[m]]
    mvals <- mvals[!is.na(mvals)]
    if (length(mvals) == 0) next

    m_props <- .valuesToHistProps(mvals, bin_breaks)
    m_counts <- .valuesToHistCounts(mvals, bin_breaks)

    monthly$psi_origin[m] <- .psi(m_props, origin_hist)
    monthly$jsd_origin[m] <- .jsdSafe(m_props, origin_hist)
    monthly$wasserstein_origin[m] <- .wassersteinSafe(mvals, origin_values)

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

  eligible_idx <- which(!monthly$insufficient_data)
  if (length(eligible_idx) >= 2) {
    bcp_input <- cbind(
      mean = monthly$m_mean[eligible_idx],
      sd = ifelse(is.na(monthly$m_sd[eligible_idx]), 0,
                  monthly$m_sd[eligible_idx])
    )
    bcp_post <- tryCatch({
      # suppressWarnings: bcp emits a benign "built under R x.y.z" message on
      # first lazy-load; DQD's outer warning handler would otherwise abort.
      bcp_res <- suppressWarnings(bcp::bcp(bcp_input))
      as.numeric(bcp_res$posterior.prob)
    }, error = function(e) {
      ParallelLogger::logWarn(sprintf(
        "bcp failed for concept=%s unit=%s: %s",
        as.character(cid), as.character(uid), conditionMessage(e)))
      rep(NA_real_, length(eligible_idx))
    })
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
    monthly$is_regime_start[eligible_idx] <- is_start
    monthly$regime_id[eligible_idx] <- cumsum(is_start)
  }

  for (rid in unique(stats::na.omit(monthly$regime_id))) {
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
      monthly$jsd_current[m] <- .jsdSafe(m_props, regime_hist)
      monthly$wasserstein_current[m] <- .wassersteinSafe(mvals, regime_values)
    }
  }

  monthly$is_anomaly <- !is.na(monthly$psi_current) &
    monthly$psi_current > driftPsiThreshold &
    !monthly$is_regime_start

  # Regime length: number of eligible months in each regime, joined back onto
  # every eligible month. Ineligible months (insufficient data) get NA.
  regime_lengths <- monthly %>%
    dplyr::filter(!is.na(regime_id)) %>%
    dplyr::count(regime_id, name = "regime_length_months")
  monthly <- monthly %>%
    dplyr::left_join(regime_lengths, by = "regime_id")

  monthly_out <- monthly %>%
    dplyr::mutate(
      measurement_concept_id = cid,
      unit_concept_id = uid
    ) %>%
    dplyr::select(
      measurement_concept_id, unit_concept_id, year_month, n_obs,
      psi_origin, wasserstein_origin, jsd_origin,
      psi_current, wasserstein_current, jsd_current,
      bcp_posterior, regime_id, regime_length_months,
      is_regime_start, is_anomaly, insufficient_data
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
  eligible_monthly <- monthly_out %>%
    dplyr::filter(!insufficient_data) %>%
    dplyr::arrange(year_month)
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
  dip_orig <- .dipTest(origin_values)
  dip_curr <- .dipTest(current_regime_values)

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
    delta_mean_pct = delta_mean_pct,
    delta_sd_pct = delta_sd_pct,
    delta_p5_pct = delta_p5_pct,
    delta_p95_pct = delta_p95_pct,
    trend_tau_mean = tr_mean$tau,
    trend_pvalue_mean = tr_mean$p_value,
    seasonality_significant = seas$significant,
    dip_pvalue_origin = dip_orig$p_value,
    dip_pvalue_current = dip_curr$p_value
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


.jsdSafe <- function(p, q) {
  tryCatch({
    val <- suppressMessages(
      philentropy::JSD(rbind(p, q), unit = "log2", est.prob = NULL, test.na = FALSE)
    )
    as.numeric(val)
  }, error = function(e) NA_real_)
}


.wassersteinSafe <- function(a, b) {
  tryCatch({
    as.numeric(transport::wasserstein1d(a, b, p = 1))
  }, error = function(e) NA_real_)
}


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


.dipTest <- function(x) {
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
  }, error = function(e) list(dip = NA_real_,
                              p_value = NA_real_,
                              is_bimodal_at_05 = NA))
}


#' Rule-based drift pattern classifier.
#'
#' Given a set of numeric features summarizing a concept's drift behavior,
#' returns a primary `pattern_type` (single string) plus `pattern_tags`
#' (semicolon-joined labels of all matching rules). Precedence for the
#' primary label goes bimodal > seasonal > trend > step > transient >
#' location > scale > tail > gradual, with `stable` short-circuiting the
#' whole thing when there is essentially no drift.
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

  # A transient is a short blip (<= 2 months) against an otherwise long
  # (>= 6 month) stable regime. PSI magnitude is not capped: real ETL
  # spikes can push PSI to 5+.
  is_transient <- !is.na(f$n_regimes) && f$n_regimes >= 2 &&
                  !is.na(f$min_regime_length_months) &&
                  f$min_regime_length_months <= 2 &&
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
