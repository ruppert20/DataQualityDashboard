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

  df <- qData %>%
    dplyr::filter(!is.na(value_as_number), !is.na(measurement_datetime)) %>%
    dplyr::mutate(year_month = format(as.Date(measurement_datetime), "%Y-%m"))

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
