# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of DataQualityDashboard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

#' Compute temporal drift metrics for a numeric CDM field.
#'
#' Extension of the numeric_stats analysis. Reuses the collected Andromeda
#' frame (qData) that numeric_stats consumes, groups it by
#' (measurement_concept_id, unit_concept_id), and computes per-month
#' distributional comparisons against both the concept's first-regime baseline
#' and each month's own containing-regime baseline.
#'
#' Detection pipeline:
#'   * Change-point segmentation: `changepoint.np::cpt.np` with PELT and a
#'     non-parametric empirical-distribution cost, applied to monthly medians.
#'     Non-parametric cost makes no distributional assumption; using medians
#'     (rather than means) as the input keeps isolated single-month outliers
#'     from opening spurious regimes. `minRegimeMonths` is enforced natively
#'     by `minseglen` during segmentation. Refs: Killick, Fearnhead & Eckley
#'     (2012) for PELT; Haynes, Fearnhead & Eckley (2017) for the
#'     non-parametric cost.
#'   * Origin baseline: the pooled raw values of the first regime (R1)
#'     identified by the change-point detector. Because R1 is single-regime
#'     by construction, the reference distribution used for all
#'     `*_origin` comparisons is coherent (does not straddle a regime
#'     boundary).
#'   * Individual-month outlier flag: Hampel filter (Hampel 1974; Iglewicz &
#'     Hoaglin 1993) applied to monthly means, with a rolling window of
#'     `hampelWindowMonths` on each side and a threshold of `hampelThreshold`
#'     scaled-MAD units. Reported per month via `is_extreme_month`.
#'   * Anomaly flag: OR of two conditions -- PSI against the current regime
#'     above `driftPsiThreshold` (0.25 = "major shift" per common industry
#'     use), or Wasserstein-1 distance to the origin baseline above a
#'     data-driven threshold defined as `wassersteinAnomalyMultiplier` times
#'     the maximum Wasserstein observed within the origin baseline itself.
#'     Extreme-month and regime-start months are excluded from the anomaly
#'     set so each month carries at most one flag.
#'
#' Per (concept, unit) group, emits three CSVs alongside the existing
#' `_stats.csv` outputs:
#'   * `<baseFilePath>_drift_monthly.csv`   -- per (concept, unit, year_month)
#'   * `<baseFilePath>_drift_summary.csv`   -- per (concept, unit)
#'   * `<baseFilePath>_drift_histogram.csv` -- per (concept, unit, year_month, bin)
#'
#' Each CSV additionally contains rows for the pooled bucket-by-unit grouping,
#' with `measurement_concept_id = "IC3_<bucket>_overall"`. This matches the
#' roll-up convention used by numeric_stats in `aggregated_stats.csv` and
#' `_numeric_time_stats.csv`. The pooled group runs the same pipeline over
#' the union of raw values across all concepts sharing a unit within the
#' bucket. Analysts filter by the `_overall` suffix on `measurement_concept_id`
#' to select the per-concept or the pooled view.
#'
#' @param qData             data frame collected from Andromeda; must contain
#'                          columns measurement_concept_id, unit_concept_id,
#'                          measurement_datetime, value_as_number.
#' @param baseFilePath      file path prefix (same convention as numeric_stats).
#' @param minMonthObs       Minimum non-NA observations for a month to be
#'                          included in drift computation. Default 30.
#' @param nBins             Requested number of quantile bins for PSI and JSD.
#'                          The realised bin count can be lower if the origin
#'                          baseline has ties at the quantile breakpoints.
#'                          Default 10.
#' @param changepointPenalty Penalty rule passed to `changepoint.np::cpt.np`.
#'                          Default "MBIC" (Modified BIC), which is
#'                          conservative and preferred for data-quality use
#'                          where false-positive regime detections are more
#'                          costly than missing minor shifts.
#' @param driftPsiThreshold PSI threshold for the anomaly flag. Default 0.25.
#' @param wassersteinAnomalyMultiplier Multiplier applied to the maximum
#'                          Wasserstein-against-origin observed among the
#'                          origin baseline months. A month whose
#'                          `wasserstein_origin` exceeds this threshold is
#'                          OR-flagged as an anomaly. Default 3.
#' @param hampelWindowMonths Half-width (in months) of the rolling Hampel
#'                          window used for single-month outlier detection.
#'                          The effective window is
#'                          `2 * hampelWindowMonths + 1`. Default 6.
#' @param hampelThreshold   Multiplier applied to the scaled rolling MAD
#'                          (MAD * 1.4826) that defines an extreme month.
#'                          Default 3.5, following Iglewicz & Hoaglin (1993).
#' @param minRegimeMonths   Minimum duration (in months) for a regime,
#'                          enforced natively by `changepoint.np::cpt.np`
#'                          via `minseglen`. Default 3.
#' @param maxOriginPoolSize Maximum number of values to keep in either the
#'                          origin reference pool (used for `*_origin`
#'                          Wasserstein / PSI / JSD comparisons) or a
#'                          current-regime pool (used for `*_current`
#'                          comparisons). When either pool holds more values
#'                          than this, a reproducibly-seeded random
#'                          subsample without replacement is drawn once per
#'                          (concept, unit, pool) and used as the reference
#'                          for every subsequent monthly comparison against
#'                          that pool. Wasserstein-1 in one dimension
#'                          converges quickly enough that a 200k-500k
#'                          subsample is within a few percent of the
#'                          full-pool value for typical measurement
#'                          distributions, and capping cuts the O(m + n)
#'                          per-month cost roughly proportionally. Pass
#'                          `Inf` to disable capping and use the full pools.
#'                          Default 500000.
#' @param memoryBudgetBytes Numeric byte figure. When the total observation
#'                          count for a (concept, unit) group would push
#'                          memory past `budget / (8 * safetyFactor)` (8 is
#'                          the size of an R double in bytes), months are
#'                          subsampled to fit. `Inf` (default) disables
#'                          subsampling.
#' @param safetyFactor      Overhead multiplier applied to the raw
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
                          nBins = 10,
                          changepointPenalty = "MBIC",
                          driftPsiThreshold = 0.25,
                          wassersteinAnomalyMultiplier = 3.0,
                          hampelWindowMonths = 6L,
                          hampelThreshold = 3.5,
                          minRegimeMonths,
                          maxOriginPoolSize = 500000L,
                          memoryBudgetBytes = Inf,
                          safetyFactor = 5,
                          driftLogLevel = "normal") {
  log_level <- .driftLogLevelNum(driftLogLevel)
  # log-normal: fires at "normal" and "verbose" levels; suppressed at "quiet"
  logn <- if (log_level >= .DRIFT_LOG_NORMAL)
    function(msg) ParallelLogger::logInfo(msg) else function(msg) invisible(NULL)
  # log-always: fires at every level (used for per-concept summary + errors)
  loga <- function(msg) ParallelLogger::logInfo(msg)

  emptyOutputs <- list(
    monthly = .emptyDriftMonthly(),
    summary = .emptyDriftSummary(),
    histogram = .emptyDriftHistogram()
  )

  .writeDriftCsvs(emptyOutputs, baseFilePath)

  if (is.null(qData) || nrow(qData) == 0) {
    loga(
      "[drift] .computeDrift entry: qData empty, nothing to do")
    return(invisible(emptyOutputs))
  }

  t_entry <- Sys.time()
  loga(sprintf(
    "[drift] .computeDrift entry: qData has %d rows, %d columns",
    nrow(qData), ncol(qData)))

  # is.finite() rejects NA, NaN, and Inf in one go — Inf would corrupt
  # mean/sd/Wasserstein without triggering the NA filter.
  t_filter <- Sys.time()
  df <- qData %>%
    dplyr::filter(is.finite(.data$value_as_number),
                  !is.na(.data$measurement_datetime)) %>%
    dplyr::mutate(year_month = .toYearMonth(.data$measurement_datetime))
  logn(sprintf(
    "[drift] filter+year_month done in %.1fs: %d rows survived (%.1f%%)",
    as.numeric(difftime(Sys.time(), t_filter, units = "secs")),
    nrow(df), 100 * nrow(df) / nrow(qData)))

  if (nrow(df) == 0) {
    loga(
      "[drift] no rows survived filter (all NA / non-finite); returning empty")
    return(invisible(emptyOutputs))
  }

  # Partition once by (concept, unit) so the downstream loop is O(nrow(df))
  # total rather than O(n_groups * nrow(df)). `split()` returns row indices
  # keyed by group; each iteration subsets `df` by index in O(group_size).
  # ASCII unit separator (0x1F) is used so the key can be split back apart
  # unambiguously.
  t_split <- Sys.time()
  key <- paste(df$measurement_concept_id,
               df$unit_concept_id,
               sep = "\x1f")
  rows_by_group <- split(seq_len(nrow(df)), key)
  n_groups <- length(rows_by_group)
  logn(sprintf(
    "[drift] split into %d (concept, unit) groups in %.1fs",
    n_groups, as.numeric(difftime(Sys.time(), t_split, units = "secs"))))

  monthly_all <- vector("list", n_groups)
  summary_all <- vector("list", n_groups)
  histogram_all <- vector("list", n_groups)

  group_keys <- names(rows_by_group)
  for (i in seq_len(n_groups)) {
    parts <- strsplit(group_keys[i], "\x1f", fixed = TRUE)[[1]]
    cid <- parts[1]
    uid <- if (length(parts) >= 2 && parts[2] != "NA") parts[2] else NA
    grp <- df[rows_by_group[[i]], , drop = FALSE]

    if (i == 1 || i %% 10 == 0 || i == n_groups) {
      logn(sprintf(
        "[drift] group %d/%d (concept=%s unit=%s, %d rows)",
        i, n_groups, as.character(cid), as.character(uid), nrow(grp)))
    }

    res <- tryCatch(
      .driftForGroup(grp, cid, uid,
                     minMonthObs, nBins,
                     changepointPenalty, driftPsiThreshold,
                     wassersteinAnomalyMultiplier,
                     hampelWindowMonths, hampelThreshold,
                     minRegimeMonths, maxOriginPoolSize,
                     memoryBudgetBytes, safetyFactor,
                     driftLogLevel = driftLogLevel),
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
    # Explicit cleanup between groups. Large concepts leave transient
    # list-columns (values per month, presorted pools, histogram rows) that
    # R's garbage collector will hold onto for several groups otherwise;
    # forcing a full GC here bounds the working set for the subsequent group.
    rm(grp, res)
    invisible(gc(verbose = FALSE, full = TRUE))
  }

  # ---- Pooled bucket-by-unit passes -----------------------------------------
  # For each unit_concept_id present in the bucket, pool the raw values
  # across all concepts sharing that unit and run the drift pipeline over
  # the pooled distribution. The output rows carry
  # `measurement_concept_id = "IC3_<bucket>_overall"`, matching the roll-up
  # convention that numeric_stats uses in aggregated_stats.csv and
  # numeric_time_stats.csv.
  #
  # Per-(concept, unit) drift and pooled drift answer different questions:
  # the per-concept view catches shifts specific to a single coding of the
  # measurement; the pooled view catches shifts that are only visible after
  # aggregating synonymous concepts (for example, when observations migrate
  # between concept ids over time while the underlying measurement is
  # unchanged). Both are emitted so the analyst can choose the appropriate
  # view for a given clinical variable.
  bucket_name <- basename(baseFilePath)
  overall_cid <- paste0(bucket_name, "_overall")
  unit_key <- ifelse(is.na(df$unit_concept_id), "\x1fNA",
                     as.character(df$unit_concept_id))
  rows_by_unit <- split(seq_len(nrow(df)), unit_key)
  n_pool <- length(rows_by_unit)
  logn(sprintf(
    "[drift] pooling into %d (bucket x unit) group(s) for %s",
    n_pool, overall_cid))
  pool_monthly <- vector("list", n_pool)
  pool_summary <- vector("list", n_pool)
  pool_histogram <- vector("list", n_pool)
  pool_keys <- names(rows_by_unit)
  for (i in seq_len(n_pool)) {
    uid <- if (pool_keys[i] == "\x1fNA") NA else pool_keys[i]
    grp <- df[rows_by_unit[[i]], , drop = FALSE]
    logn(sprintf(
      "[drift] pool %d/%d (%s unit=%s, %d rows across %d concepts)",
      i, n_pool, overall_cid, as.character(uid), nrow(grp),
      length(unique(grp$measurement_concept_id))))
    res <- tryCatch(
      .driftForGroup(grp, overall_cid, uid,
                     minMonthObs, nBins,
                     changepointPenalty, driftPsiThreshold,
                     wassersteinAnomalyMultiplier,
                     hampelWindowMonths, hampelThreshold,
                     minRegimeMonths, maxOriginPoolSize,
                     memoryBudgetBytes, safetyFactor,
                     driftLogLevel = driftLogLevel),
      error = function(e) {
        ParallelLogger::logWarn(sprintf(
          "Pooled drift failed for %s unit=%s: %s",
          overall_cid, as.character(uid), conditionMessage(e)))
        NULL
      }
    )
    if (!is.null(res)) {
      pool_monthly[[i]] <- res$monthly
      pool_summary[[i]] <- res$summary
      pool_histogram[[i]] <- res$histogram
    }
    rm(grp, res)
    invisible(gc(verbose = FALSE, full = TRUE))
  }

  outputs <- list(
    monthly = dplyr::bind_rows(c(monthly_all, pool_monthly)),
    summary = dplyr::bind_rows(c(summary_all, pool_summary)),
    histogram = dplyr::bind_rows(c(histogram_all, pool_histogram))
  )

  .writeDriftCsvs(outputs, baseFilePath)
  loga(sprintf(
    "[drift] .computeDrift exit: %d per-concept + %d pooled groups processed in %.1fs total",
    n_groups, n_pool,
    as.numeric(difftime(Sys.time(), t_entry, units = "secs"))))
  invisible(outputs)
}


#' Compute the full drift pipeline for a single (concept, unit) group or
#' pooled bucket-by-unit group.
#'
#' Runs the following steps in order on the pre-filtered rows for one group:
#'   1. Memory-adaptive per-month subsampling (when a memory budget is set).
#'   2. Monthly aggregation to per-month n, mean, median, sd, and list-column
#'      of raw values.
#'   3. Hampel filter on monthly means -> `is_extreme_month`.
#'   4. Non-parametric change-point segmentation on monthly medians via
#'      `changepoint.np::cpt.np` -> `regime_id`, `is_regime_start`.
#'   5. Origin baseline construction from the first regime's pooled values,
#'      then per-month PSI / Wasserstein-1 / JSD against that baseline.
#'   6. Per-month PSI / Wasserstein-1 / JSD against each month's containing
#'      regime's baseline.
#'   7. Anomaly flag `is_anomaly` (PSI-current above threshold OR
#'      Wasserstein-origin above data-driven threshold; extreme and
#'      regime-start months excluded).
#'   8. Trend, seasonality, and bimodality diagnostics; rule-based pattern
#'      classification; priority score.
#'
#' @param grp Data frame of the rows that belong to this group (already
#'   filtered by the caller). Must contain columns value_as_number,
#'   year_month, measurement_concept_id.
#' @param cid `measurement_concept_id` label for the emitted rows. For a
#'   pooled group this is `"IC3_<bucket>_overall"`.
#' @param uid `unit_concept_id` label for the emitted rows.
#' @param minMonthObs,nBins,changepointPenalty,driftPsiThreshold,wassersteinAnomalyMultiplier,hampelWindowMonths,hampelThreshold,minRegimeMonths,memoryBudgetBytes,safetyFactor,driftLogLevel
#'   See `.computeDrift`.
#'
#' @return List with elements `monthly`, `summary`, `histogram`, or NULL if
#'   the group cannot be processed (fewer than two monthly rows, no eligible
#'   month, degenerate baseline).
#'
#' @keywords internal
.driftForGroup <- function(grp, cid, uid,
                           minMonthObs, nBins,
                           changepointPenalty, driftPsiThreshold,
                           wassersteinAnomalyMultiplier,
                           hampelWindowMonths, hampelThreshold,
                           minRegimeMonths, maxOriginPoolSize,
                           memoryBudgetBytes = Inf,
                           safetyFactor = 5,
                           driftLogLevel = "normal") {
  log_level <- .driftLogLevelNum(driftLogLevel)
  # logn (normal): suppressed at "quiet"; loga (always): fires at every level
  # (used for concept-summary lines and abort reasons users always need).
  logn <- if (log_level >= .DRIFT_LOG_NORMAL)
    function(msg) ParallelLogger::logInfo(msg) else function(msg) invisible(NULL)
  loga <- function(msg) ParallelLogger::logInfo(msg)

  if (nrow(grp) == 0) return(NULL)

  # Consistent prefix so operators can `grep '\[drift/CID]'` in a large log
  # file to trace one concept, or `grep '\[drift]'` for all drift activity.
  tag <- sprintf("[drift/%s/%s]", as.character(cid), as.character(uid))
  gc_reset <- gc(verbose = FALSE, reset = TRUE)
  t0 <- Sys.time()
  logn(sprintf(
    "%s entry: %d rows (before monthly grouping)",
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
    loga(sprintf(
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

  logn(sprintf(
    "%s building monthly_raw list-column (%d rows, %d months)",
    tag, nrow(grp), n_months))
  monthly_raw <- grp %>%
    dplyr::group_by(.data$year_month) %>%
    dplyr::summarise(
      n_obs_used = dplyr::n(),
      m_mean = mean(.data$value_as_number, na.rm = TRUE),
      m_median = stats::median(.data$value_as_number, na.rm = TRUE),
      m_sd = stats::sd(.data$value_as_number, na.rm = TRUE),
      values = list(.data$value_as_number),
      .groups = "drop"
    ) %>%
    dplyr::arrange(.data$year_month) %>%
    dplyr::mutate(
      n_obs = as.integer(n_obs_original_by_month[.data$year_month])
    )

  if (nrow(monthly_raw) < 2) {
    loga(sprintf(
      "%s abort: only %d monthly rows after grouping (need >= 2)",
      tag, nrow(monthly_raw)))
    return(NULL)
  }

  # Eligibility is based on ORIGINAL n_obs — subsampling never demotes a month
  # into insufficient-data territory because the cap is floored at minMonthObs.
  monthly <- monthly_raw %>%
    dplyr::mutate(insufficient_data = .data$n_obs < minMonthObs)

  monthly$psi_origin <- NA_real_
  monthly$wasserstein_origin <- NA_real_
  monthly$jsd_origin <- NA_real_
  monthly$psi_current <- NA_real_
  monthly$wasserstein_current <- NA_real_
  monthly$jsd_current <- NA_real_
  monthly$regime_id <- NA_integer_
  monthly$is_regime_start <- FALSE
  monthly$is_extreme_month <- FALSE
  monthly$is_anomaly <- FALSE

  eligible_idx <- which(!monthly$insufficient_data)
  if (length(eligible_idx) == 0) {
    loga(sprintf(
      "%s abort: no month has >= %d observations (all insufficient)",
      tag, minMonthObs))
    return(NULL)
  }

  # ---- Individual-month outlier flag (Hampel filter on monthly means) --------
  # Hampel filter (Hampel 1974; Iglewicz & Hoaglin 1993). Compares each
  # month's mean against a rolling median-and-MAD window and flags points
  # more than `hampelThreshold` scaled-MAD units from the local median. Runs
  # on monthly means (sensitive to extreme values), in parallel with the
  # change-point detector below (which runs on monthly medians and is
  # therefore robust to isolated outliers). Together, the two produce
  # separate flags for isolated-month outliers and for sustained
  # distributional shifts.
  if (length(eligible_idx) >= 3) {
    hampel_flags <- .hampelFilter(
      monthly$m_mean[eligible_idx],
      window_half = hampelWindowMonths,
      threshold = hampelThreshold
    )
    monthly$is_extreme_month[eligible_idx] <- hampel_flags
    logn(sprintf(
      "%s hampel outlier stage: %d extreme month(s) flagged (window=%d, threshold=%.1f MADs)",
      tag, sum(hampel_flags, na.rm = TRUE),
      hampelWindowMonths, hampelThreshold))
  }

  # ---- Change-point segmentation (non-parametric PELT on monthly medians) ---
  # PELT (Killick, Fearnhead & Eckley 2012) with the non-parametric empirical
  # distribution cost (Haynes, Fearnhead & Eckley 2017) as implemented in
  # `changepoint.np::cpt.np`. Uses monthly medians as input so isolated
  # single-month outliers do not open spurious regimes. `minseglen` is
  # honoured natively during segmentation.
  min_cpt_months <- 6L
  if (length(eligible_idx) >= min_cpt_months) {
    logn(sprintf(
      "%s cpt.np: PELT non-parametric on %d monthly medians (penalty=%s, minseglen=%d)",
      tag, length(eligible_idx), changepointPenalty, minRegimeMonths))
    cpt_start <- Sys.time()
    cpt_positions <- tryCatch({
      medians_here <- monthly$m_median[eligible_idx]
      # A constant or near-constant median series has no meaningful
      # change-point structure; return no change points rather than let
      # changepoint.np error.
      if (length(unique(medians_here[!is.na(medians_here)])) < 2) {
        integer(0)
      } else {
        fit <- changepoint.np::cpt.np(
          data = medians_here,
          method = "PELT",
          penalty = changepointPenalty,
          minseglen = as.integer(minRegimeMonths)
        )
        cp <- changepoint::cpts(fit)
        # `cpts()` returns the last position of each pre-change segment.
        # Drop any trailing end-of-series position so `cp + 1` in the
        # regime-assignment step never exceeds the series length.
        cp <- cp[cp < length(medians_here)]
        as.integer(cp)
      }
    }, error = function(e) {
      ParallelLogger::logWarn(sprintf(
        "%s cpt.np failed (%d months, median range [%.4g, %.4g]): %s. Falling back to a single-regime segmentation.",
        tag, length(eligible_idx),
        min(monthly$m_median[eligible_idx], na.rm = TRUE),
        max(monthly$m_median[eligible_idx], na.rm = TRUE),
        conditionMessage(e)))
      integer(0)
    })
    logn(sprintf(
      "%s cpt.np complete (%.1fs, %d change point(s))",
      tag, as.numeric(difftime(Sys.time(), cpt_start, units = "secs")),
      length(cpt_positions)))
  } else {
    if (length(eligible_idx) > 0) {
      loga(sprintf(
        "%s cpt.np skipped (only %d eligible months, need >= %d); treating whole series as one regime",
        tag, length(eligible_idx), min_cpt_months))
    }
    cpt_positions <- integer(0)
  }

  # Regime assignment. change point at position k -> new regime starts at k+1.
  # First eligible month is always a regime start by definition.
  if (length(eligible_idx) > 0) {
    is_start <- rep(FALSE, length(eligible_idx))
    is_start[1] <- TRUE
    if (length(cpt_positions) > 0) {
      new_starts <- cpt_positions + 1L
      new_starts <- new_starts[new_starts <= length(eligible_idx)]
      is_start[new_starts] <- TRUE
    }
    monthly$is_regime_start[eligible_idx] <- is_start
    monthly$regime_id[eligible_idx] <- cumsum(is_start)
  }

  # ---- Origin baseline: pooled values of the first regime -------------------
  # The origin baseline is the pooled raw values of the first regime the
  # change-point detector identified. Using R1 (rather than a fixed-length
  # calendar window) ensures the reference distribution used by all
  # `*_origin` comparisons is single-regime by construction and therefore
  # never straddles a segmentation boundary.
  r1_idx <- which(monthly$regime_id == 1L & !monthly$insufficient_data)
  origin_months <- monthly$year_month[r1_idx]
  origin_values <- unlist(monthly$values[r1_idx], use.names = FALSE)
  origin_values <- origin_values[!is.na(origin_values)]
  if (length(origin_values) < minMonthObs) {
    loga(sprintf(
      "%s abort: R1 baseline pool has only %d values (need >= %d)",
      tag, length(origin_values), minMonthObs))
    return(NULL)
  }

  # Cap the origin reference pool. Wasserstein-1 in one dimension converges
  # quickly enough that a subsample of a few hundred thousand values is
  # within a few percent of the full-pool value for typical measurement
  # distributions, and capping cuts the per-month O(m + n) cost
  # proportionally. The cap fires once per group; the same subsample is
  # then used as the reference for every subsequent monthly comparison in
  # this group. The RNG is seeded deterministically from (cid, uid,
  # maxOriginPoolSize) so results are reproducible across runs and
  # independent across groups.
  origin_n_obs_available <- length(origin_values)
  origin_pool_capped <- FALSE
  if (is.finite(maxOriginPoolSize) &&
      origin_n_obs_available > maxOriginPoolSize) {
    seed_str <- paste(as.character(cid), as.character(uid),
                      as.integer(maxOriginPoolSize), sep = "|")
    set.seed(sum(utf8ToInt(seed_str)) %% .Machine$integer.max)
    origin_values <- sample(origin_values,
                            size = as.integer(maxOriginPoolSize),
                            replace = FALSE)
    origin_pool_capped <- TRUE
    logn(sprintf(
      "%s origin pool capped: %d -> %d values (maxOriginPoolSize = %d)",
      tag, origin_n_obs_available, length(origin_values),
      as.integer(maxOriginPoolSize)))
  }

  bin_breaks <- .quantileBinBreaks(origin_values, nBins)
  if (is.null(bin_breaks)) {
    loga(sprintf(
      "%s abort: could not construct quantile bins from R1 baseline",
      tag))
    return(NULL)
  }
  n_bins_actual <- length(bin_breaks) - 1L
  # Pre-sort the origin pool ONCE. All downstream Wasserstein calls against
  # origin_values use .wasserstein1PresortedPool, which is byte-for-byte
  # identical to transport::wasserstein1d(month, origin_values) but skips the
  # O(n log n) sort transport would redo per call.
  origin_values_sorted <- sort(origin_values)
  origin_hist <- .valuesToHistProps(origin_values, bin_breaks)
  logn(sprintf(
    "%s R1 baseline: %d months, %d values, %d bins",
    tag, length(origin_months), length(origin_values), n_bins_actual))

  # ---- Origin-comparison loop (PSI / Wasserstein / JSD vs R1) --------------
  hist_rows <- vector("list", nrow(monthly))
  phase_start <- Sys.time()
  logn(sprintf(
    "%s origin-comparison phase: %d eligible months x [PSI, JSD, Wasserstein]",
    tag, length(eligible_idx)))
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
    monthly$wasserstein_origin[m] <- .wasserstein1PresortedPool(
      mvals, origin_values_sorted,
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
  logn(sprintf(
    "%s origin-comparison phase complete (%.1fs)",
    tag, as.numeric(difftime(Sys.time(), phase_start, units = "secs"))))

  regime_ids <- unique(stats::na.omit(monthly$regime_id))
  logn(sprintf(
    "%s current-regime phase: pooling values across %d regime(s)",
    tag, length(regime_ids)))
  regime_start <- Sys.time()
  for (rid in regime_ids) {
    regime_rows <- which(monthly$regime_id == rid & !is.na(monthly$regime_id))
    regime_values <- unlist(monthly$values[regime_rows], use.names = FALSE)
    regime_values <- regime_values[!is.na(regime_values)]
    if (length(regime_values) < minMonthObs) next
    # Apply the same size cap to the regime reference pool as to the origin
    # pool, following the same rationale: Wasserstein-1 convergence in 1D
    # does not require the full pool once it is well over the cap, and
    # capping keeps the per-month O(m + n) cost bounded for large regimes.
    # A separate seed component (`rid`) makes each regime's subsample
    # independent of the origin subsample and other regimes' subsamples,
    # while remaining reproducible.
    if (is.finite(maxOriginPoolSize) &&
        length(regime_values) > maxOriginPoolSize) {
      seed_str <- paste(as.character(cid), as.character(uid),
                        as.integer(maxOriginPoolSize), "rid", rid, sep = "|")
      set.seed(sum(utf8ToInt(seed_str)) %% .Machine$integer.max)
      regime_values <- sample(regime_values,
                              size = as.integer(maxOriginPoolSize),
                              replace = FALSE)
      logn(sprintf(
        "%s current-regime R%d pool capped at %d values",
        tag, as.integer(rid), as.integer(maxOriginPoolSize)))
    }
    regime_hist <- .valuesToHistProps(regime_values, bin_breaks)
    # Pre-sort the regime pool once so every monthly Wasserstein call within
    # this regime skips the redundant sort of the pool.
    regime_values_sorted <- sort(regime_values)

    for (m in regime_rows) {
      mvals <- monthly$values[[m]]
      mvals <- mvals[!is.na(mvals)]
      if (length(mvals) == 0) next
      m_props <- .valuesToHistProps(mvals, bin_breaks)
      monthly$psi_current[m] <- .psi(m_props, regime_hist)
      monthly$jsd_current[m] <- .jsdSafe(m_props, regime_hist,
                                         ctx = tag,
                                         month = monthly$year_month[m])
      monthly$wasserstein_current[m] <- .wasserstein1PresortedPool(
        mvals, regime_values_sorted,
        ctx = tag, month = monthly$year_month[m])
    }
  }
  logn(sprintf(
    "%s current-regime phase complete (%.1fs)",
    tag, as.numeric(difftime(Sys.time(), regime_start, units = "secs"))))

  # ---- Anomaly flag: OR of PSI-current and Wasserstein-origin ---------------
  # Two complementary signals:
  #   * PSI against the current regime's baseline is the interpretable,
  #     industry-standard signal for "distributional shape has shifted from
  #     where this regime lives"; the 0.25 threshold is the common
  #     "major shift" convention.
  #   * PSI operates on discrete bins, so all values above the top quantile
  #     edge fall into a single bucket and PSI cannot distinguish moderate
  #     from extreme tail excursions. Wasserstein-1 to the origin baseline
  #     is continuous and tail-sensitive; adding it as an OR condition
  #     captures tail excursions without changing the PSI threshold.
  #
  # Wasserstein threshold: `wassersteinAnomalyMultiplier` times the maximum
  # Wasserstein observed within the origin baseline months. This is
  # self-calibrating; for a bucket in which within-baseline Wasserstein is
  # typically ~40, an anomaly month must exceed ~120 at multiplier 3.
  #
  # Extreme-month rows (Hampel-flagged) are excluded from `is_anomaly` so
  # each month carries at most one of the three flag categories
  # (`is_regime_start`, `is_extreme_month`, `is_anomaly`).
  w_baseline <- monthly$wasserstein_origin[
    monthly$year_month %in% origin_months]
  w_baseline_max <- suppressWarnings(max(w_baseline, na.rm = TRUE))
  if (!is.finite(w_baseline_max) || w_baseline_max <= 0) {
    # Origin months have effectively zero Wasserstein against themselves
    # (for example, when there is only a single origin month). Fall back to a
    # MAD-based threshold on the full eligible Wasserstein series.
    w_all <- monthly$wasserstein_origin[!is.na(monthly$wasserstein_origin)]
    if (length(w_all) >= 3) {
      w_med <- stats::median(w_all)
      w_mad <- 1.4826 * stats::mad(w_all, constant = 1)
      w_threshold <- w_med + wassersteinAnomalyMultiplier * w_mad
    } else {
      # Fewer than 3 usable Wasserstein values; disable the Wasserstein arm.
      w_threshold <- Inf
    }
  } else {
    w_threshold <- wassersteinAnomalyMultiplier * w_baseline_max
  }
  logn(sprintf(
    "%s anomaly threshold: PSI>%.2f OR Wasserstein_origin>%.2f",
    tag, driftPsiThreshold, w_threshold))

  psi_hit <- !is.na(monthly$psi_current) &
    monthly$psi_current > driftPsiThreshold
  wass_hit <- !is.na(monthly$wasserstein_origin) &
    monthly$wasserstein_origin > w_threshold
  monthly$is_anomaly <- (psi_hit | wass_hit) &
    !monthly$is_regime_start &
    !monthly$is_extreme_month

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
      "regime_id", "regime_length_months",
      "is_regime_start", "is_extreme_month", "is_anomaly",
      "insufficient_data"
    )

  regime_starts <- monthly_out$year_month[monthly_out$is_regime_start]
  regime_start_lengths <- monthly_out$regime_length_months[monthly_out$is_regime_start]
  anomalous <- monthly_out$year_month[monthly_out$is_anomaly]
  extreme_months <- monthly_out$year_month[monthly_out$is_extreme_month]
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
    origin_n_obs_available = origin_n_obs_available,
    origin_pool_capped = origin_pool_capped,
    n_bins = n_bins_actual,
    n_regimes = n_regimes,
    regime_change_months = paste(regime_starts, collapse = ";"),
    regime_lengths_months = paste(regime_start_lengths, collapse = ";"),
    current_regime_start = current_regime_start,
    current_regime_length_months = current_regime_length,
    n_anomalous_months = length(anomalous),
    anomalous_months = paste(anomalous, collapse = ";"),
    n_extreme_months = length(extreme_months),
    extreme_months = paste(extreme_months, collapse = ";"),
    wasserstein_anomaly_threshold =
      if (is.finite(w_threshold)) w_threshold else NA_real_,
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
  logn(sprintf(
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
  logn(sprintf(
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

  # Subsampling audit: record how much of the raw data actually fed the
  # drift calculation. When no subsampling occurred, `n_obs_used == n_obs`
  # and `pct_obs_used == 100`.
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

  # gc() returns a 2-row matrix (Ncells / Vcells). Column layout differs by
  # R version and platform:
  #   R >= 4.4 Windows: 7 cols (includes "limit (Mb)" between trigger and max)
  #   R >= 4.4 non-Windows: 6 cols (no limit column)
  #   Older R: 6 cols
  # "used (Mb)" is always column 2. "max used (Mb)" is always the LAST column
  # regardless of layout, so index by ncol() to stay portable.
  gc_final <- gc(verbose = FALSE)
  mem_now_mb <- sum(gc_final[, 2])
  mem_peak_mb <- sum(gc_final[, ncol(gc_final)])
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  loga(sprintf(
    paste("%s complete: %.1fs, %d regimes, %d anomalies, R memory now",
          "%.0f MB (peak this concept %.0f MB), subsampled=%s (%.1f%% of obs used)"),
    tag, elapsed,
    summary_out$n_regimes, summary_out$n_anomalous_months,
    mem_now_mb, mem_peak_mb,
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


# Each wrapper accepts optional `ctx` and `month` arguments used only for
# diagnostic logging. When an underlying native call errors (or crashes the
# session), the wrapper records which concept, which month, and which call
# site produced the failure, together with the input sizes. Without this
# context, a native-side segfault would produce only R's generic "session
# terminated" message.
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


# Log-level dispatcher. Numeric levels: 1 = quiet, 2 = normal, 3 = verbose.
# .driftForGroup / .computeDrift accept driftLogLevel as an integer and gate
# each ParallelLogger::logInfo call by threshold. Errors and warnings always
# emit regardless of level — those go through logWarn/logError.
.DRIFT_LOG_QUIET   <- 1L
.DRIFT_LOG_NORMAL  <- 2L
.DRIFT_LOG_VERBOSE <- 3L

.driftLogLevelNum <- function(level) {
  if (is.numeric(level) && length(level) == 1) return(as.integer(level))
  switch(as.character(level),
         quiet = .DRIFT_LOG_QUIET,
         normal = .DRIFT_LOG_NORMAL,
         verbose = .DRIFT_LOG_VERBOSE,
         .DRIFT_LOG_NORMAL)   # unknown -> normal
}


# Wasserstein-1 with a presorted pool. Numerically identical to
# `transport::wasserstein1d(a, b_original, p = 1)` when
# `b_sorted == sort(b_original)` and all weights are 1, but skips the
# redundant sort of `b`. The optimisation matters because a large origin
# pool is compared against many months per group; sorting the pool once
# saves the cost of one sort per comparison.
.wasserstein1PresortedPool <- function(a, b_sorted,
                                       ctx = NULL, month = NULL) {
  tryCatch({
    m <- length(a)
    n <- length(b_sorted)
    if (m == 0 || n == 0) return(NA_real_)
    # Equal-size fast path (same code branch transport takes)
    if (m == n) {
      return(mean(abs(b_sorted - sort(a))))
    }
    # Unweighted, unequal-size branch — same operations as transport, minus
    # the redundant sort of b.
    wa <- rep(1, m)
    wb <- rep(1, n)
    orda <- order(a)
    a <- a[orda]
    wa <- wa[orda]
    b <- b_sorted
    ua <- (wa / sum(wa))[-m]
    ub <- (wb / sum(wb))[-n]
    cua <- c(cumsum(ua))
    cub <- c(cumsum(ub))
    arep <- graphics::hist(cub, breaks = c(-Inf, cua, Inf),
                            plot = FALSE)$counts + 1
    brep <- graphics::hist(cua, breaks = c(-Inf, cub, Inf),
                            plot = FALSE)$counts + 1
    aa <- rep(a, times = arep)
    bb <- rep(b, times = brep)
    uu <- sort(c(cua, cub))
    uu0 <- c(0, uu)
    uu1 <- c(uu, 1)
    sum((uu1 - uu0) * abs(bb - aa))
  }, error = function(e) {
    if (!is.null(ctx)) {
      ParallelLogger::logWarn(sprintf(
        "%s Wasserstein (presorted) failed at %s (a len=%d, b len=%d): %s",
        ctx, month %||% "?", length(a), length(b_sorted),
        conditionMessage(e)))
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


# Cross-platform available-memory probe used by the "auto" memory budget
# resolution. On Linux, reads MemAvailable from /proc/meminfo (the kernel's
# estimate of memory an application can allocate without swapping). On
# macOS, sums free and inactive pages reported by `vm_stat`. On Windows,
# queries FreePhysicalMemory via `wmic`. Returns NA on any failure; the
# caller falls back to disabling subsampling in that case.
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


# Minimum regime length is enforced during segmentation via
# `changepoint.np::cpt.np(..., minseglen = minRegimeMonths)`, so no post-hoc
# merge of short regimes is required.


#' Hampel filter for isolated-outlier detection on a time series.
#'
#' For each position, compute the median and MAD of the surrounding
#' `window_half` neighbours on either side (excluding the position itself),
#' scale the MAD by 1.4826 to be Gaussian-consistent, and flag the position
#' as an outlier if it lies more than `threshold` scaled MADs from the local
#' median. Reference: Hampel (1974); Iglewicz & Hoaglin (1993) discuss the
#' 3.5-MAD default as the modified-z-score outlier threshold.
#'
#' @param x A numeric vector.
#' @param window_half Half-width of the rolling window (number of neighbours
#'                    on each side). Effective window is `2 * window_half + 1`.
#' @param threshold Number of scaled-MAD units defining "outlier".
#'
#' @return Logical vector of the same length as `x`.
#'
#' @keywords internal
.hampelFilter <- function(x, window_half = 6L, threshold = 3.5) {
  n <- length(x)
  flags <- rep(FALSE, n)
  if (n < 3L) return(flags)
  for (i in seq_len(n)) {
    lo <- max(1L, i - window_half)
    hi <- min(n, i + window_half)
    neighbours <- x[setdiff(lo:hi, i)]
    neighbours <- neighbours[!is.na(neighbours)]
    if (length(neighbours) < 3L) next
    med <- stats::median(neighbours)
    mad_val <- 1.4826 * stats::mad(neighbours, constant = 1)
    if (!is.finite(mad_val) || mad_val <= 0) next
    if (!is.na(x[i]) && abs(x[i] - med) > threshold * mad_val) {
      flags[i] <- TRUE
    }
  }
  flags
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
    regime_id = integer(0),
    regime_length_months = integer(0),
    is_regime_start = logical(0),
    is_extreme_month = logical(0),
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
    origin_n_obs_available = integer(0),
    origin_pool_capped = logical(0),
    n_bins = integer(0),
    n_regimes = integer(0),
    regime_change_months = character(0),
    regime_lengths_months = character(0),
    current_regime_start = character(0),
    current_regime_length_months = integer(0),
    n_anomalous_months = integer(0),
    anomalous_months = character(0),
    n_extreme_months = integer(0),
    extreme_months = character(0),
    wasserstein_anomaly_threshold = numeric(0),
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
