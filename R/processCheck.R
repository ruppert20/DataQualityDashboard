# Copyright 2025 Observational Health Data Sciences and Informatics
#
# This file is part of DataQualityDashboard
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Calculate the statistical mode of a numeric vector.
#'
#' @param x A numeric vector.
#'
#' @return The most-frequent value in `x`, or `NA` if `x` is empty.
#'
#' @keywords internal
calculate_mode <- function(x) {
  tbl <- table(x)
  if (length(tbl) == 0) {
    return(NA)
  }
  modes <- tbl == max(tbl)
  as.numeric(names(modes)[which.max(modes)])
}

#' Flatten a possibly chained condition into a single-line message.
#'
#' rlang and dplyr wrap errors so that conditionMessage() returns only the
#' outermost frame (e.g. "In argument: `month = format(...)`") while the actual
#' cause is held in $parent. Logging only the outer message discards the reason
#' the check failed. Walks the whole chain and collapses it onto one line so it
#' stays readable in the tab-delimited ParallelLogger output.
#'
#' @param e   A condition object
#'
#' @return A single-line character string containing the full cause chain
#'
#' @keywords internal
#'
.flattenConditionMessage <- function(e) {
  msgs <- character(0)
  cnd <- e
  while (inherits(cnd, "condition")) {
    msg <- tryCatch(conditionMessage(cnd), error = function(...) NULL)
    if (length(msg) > 0 && any(nzchar(msg))) {
      msg <- trimws(gsub("\\s+", " ", paste(msg, collapse = " ")))
      # conditionMessage() on an rlang error already renders the whole chain, so
      # only append a parent that adds something the outer message did not say
      if (nzchar(msg) && !any(grepl(msg, msgs, fixed = TRUE))) {
        msgs <- c(msgs, msg)
      }
    }
    cnd <- cnd$parent
  }
  if (length(msgs) == 0) {
    return("<no condition message>")
  }
  paste(msgs, collapse = " Caused by: ")
}

#' Internal function to send the fully qualified sql to the database and return
#' the numerical result.
#'
#' @param connection                A connection for connecting to the CDM database using the DatabaseConnector::connect(connectionDetails) function.
#' @param connectionDetails         A connectionDetails object for connecting to the CDM database.
#' @param check                     The data quality check
#' @param checkDescription          The description of the data quality check
#' @param sql                       The fully qualified sql for the data quality check
#' @param outputFolder              The folder to output logs and SQL files to.
#' @param patEncSql                 The SQL for patient and encounter statistics
#' @param cdmVersion                The CDM version (e.g., "5.3", "5.4")
#' @param resume                    If TRUE and a per-check Andromeda cache file already exists at `<baseFilePath>.andromeda`, load it and skip re-running the SQL query. If FALSE, or if no cache file is present, run the query and (re)write the cache. Cached Andromeda files are keyed by check name; changing the SQL for a check requires deleting the corresponding `.andromeda` file to avoid loading stale results.
#' @param computeDrift              Whether to run the temporal drift extension on numeric checks. Default TRUE.
#' @param minRegimeMonths           Minimum months required for a regime, enforced natively by changepoint.np during segmentation (minseglen). Default 3.
#' @param maxOriginPoolSize         Maximum size of the origin reference pool for Wasserstein / PSI / JSD comparisons; see `.computeDrift` for details. Default 500000.
#' @param driftMemoryBudgetBytes    Per-worker memory budget (in bytes) used to decide when to subsample large months.
#' @param driftLogLevel             Verbosity of drift-computation log output: "quiet" (concept summary + errors only), "normal" (default; all phase logs), or "verbose".
#'
#' @return A dataframe containing the check results
#'
#' @keywords internal
.processCheck <- function(connection,
                          connectionDetails,
                          check,
                          checkDescription,
                          sql,
                          outputFolder,
                          patEncSql,
                          cdmVersion,
                          resume,
                          computeDrift,
                          minRegimeMonths,
                          maxOriginPoolSize,
                          driftMemoryBudgetBytes,
                          driftLogLevel) {
  singleThreaded <- TRUE
  start <- Sys.time()
  if (is.null(connection)) {
    singleThreaded <- FALSE
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection = connection))
  }

  errorReportFile <- file.path(
    outputFolder, "errors",
    sprintf(
      "%s_%s_%s_%s.txt",
      checkDescription$checkLevel,
      checkDescription$checkName,
      check["cdmTableName"],
      check["cdmFieldName"]
    )
  )
  tryCatch(
    expr = {
      if (singleThreaded) {
        if (.needsAutoCommit(connectionDetails = connectionDetails, connection = connection)) {
          rJava::.jcall(connection@jConnection, "V", "setAutoCommit", TRUE)
        }
      }
      # Extension-check branch: numeric-stats, value-as-concept-stats, and
      # concept-census checks tag their SQL with the sentinel XXXSAVE_FULL_RESULTXXX
      # so that the full query result is materialised into an Andromeda cache
      # file, aggregated in R, and (for numeric checks) fed to the drift
      # extension. Other checks fall through to the plain violation-count path
      # in the `else` block below.
      if (grepl('XXXSAVE_FULL_RESULTXXX', sql, TRUE)) {

        # Person / encounter denominators for the check's cohort, used to
        # compute percent_patients and percent_visits downstream.
        patEncResult <- DatabaseConnector::querySql(
                                                    connection = connection, sql = patEncSql,
                                                    errorReportFile = errorReportFile,
                                                    snakeCaseToCamelCase = TRUE
      )

        # Extract the check_name from the sentinel `XXXQUERYNAME___<name>XXX`
        # marker that runCheck.R renders into the SQL. Appended with the CDM
        # table name so filenames disambiguate between checks that share a
        # concept set across the MEASUREMENT and OBSERVATION domains.
        query_name <- stringr::str_extract(sql, "XXXQUERYNAME___[A-z_0-9_]+XXX")
        query_name <- stringr::str_replace(query_name, "^XXXQUERYNAME___", "")
        query_name <- stringr::str_replace(query_name, "XXX$", "")
        check_name <- paste(query_name,
                            tolower(check["cdmTableName"]),
                            sep = "_")

        # Lowercased so column names in the Andromeda result table match the
        # unquoted references used by the R-side dplyr chains below
        # (value_as_number, person_id, measurement_datetime, ...). SqlRender
        # has already handled the dialect translation and date-column
        # substitution before this point.
        querySQL <- tolower(sql)

        # define base file path
        baseFilePath <- file.path(outputFolder, check_name)
        andromedaFile <- paste0(baseFilePath, ".andromeda")

        if (resume && file.exists(andromedaFile)) {
          # Reuse a previously materialised query result. Assumes the cache
          # matches the current SQL for this check; callers who change SQL
          # should delete the corresponding .andromeda file.
          ParallelLogger::logInfo(sprintf("Resuming %s from Andromeda", check_name))
          andromedaObject <- Andromeda::loadAndromeda(andromedaFile)
        } else {
          ParallelLogger::logInfo(sprintf("Running %s Query", check_name))

          # Stream query results into an Andromeda-backed table so the
          # working set never has to fit fully in R memory. `appendToTable`
          # is FALSE so a fresh table replaces any prior content.
          andromedaObject <- Andromeda::andromeda()

          DatabaseConnector::querySqlToAndromeda(
            connection = connection,
            sql = querySQL,
            andromeda = andromedaObject,
            andromedaTableName = "query_result",
            errorReportFile = errorReportFile,
            snakeCaseToCamelCase = FALSE,
            appendToTable = FALSE
          )

          # Persist to disk so a subsequent run with `resume = TRUE` can skip
          # the query. `maintainConnection = TRUE` keeps the returned
          # Andromeda object usable for the aggregations below.
          Andromeda::saveAndromeda(andromeda = andromedaObject,
                                  fileName = andromedaFile,
                                  maintainConnection = TRUE,
                                  overwrite = TRUE)
          ParallelLogger::logInfo(sprintf("  Query complete, data saved to Andromeda"))
        }
        on.exit(Andromeda::close(andromedaObject), add = TRUE)

        # Materialise the query result into an in-memory data frame for the
        # per-concept aggregations below. Some of the summary statistics
        # (quantile with R's default interpolation, `mad`, `calculate_mode`)
        # are computed R-side rather than in the backing store so their
        # semantics are portable across Andromeda backends and match the
        # behaviour of base R.
        ParallelLogger::logInfo(sprintf("Collecting data from Andromeda for %s", check_name))
        qData <- andromedaObject$query_result %>% dplyr::collect()
        rowCount <- nrow(qData)

        if (grepl('VALUE_AS_NUMBER_CHECK', sql, TRUE)){
          # calculate stats
          ParallelLogger::logInfo(sprintf("Calculating Numeric summary for %s", check_name))

          if (rowCount == 0) {
            ParallelLogger::logInfo(sprintf("No data found for %s, skipping stats calculation", check_name))
            qStats <- data.frame(
              measurement_concept_id = character(0),
              unit_concept_id = character(0),
              min = numeric(0),
              percentile_5 = numeric(0),
              percentile_25 = numeric(0),
              median = numeric(0),
              mean = numeric(0),
              mode = numeric(0),
              percentile_75 = numeric(0),
              percentile_95 = numeric(0),
              max = numeric(0),
              standard_deviation = numeric(0),
              median_absolute_deviation = numeric(0),
              number_of_measurements = integer(0),
              number_of_patients = integer(0),
              number_of_visits = integer(0),
              percent_patients = numeric(0),
              percent_visits = numeric(0),
              percent_missing = numeric(0),
              min_date = as.POSIXct(character(0)),
              max_date = as.POSIXct(character(0))
            )
          } else {
            # Compute statistics in R (data already collected from Andromeda)
            qStats <- rbind(qData %>%
                            dplyr::group_by(measurement_concept_id, unit_concept_id) %>%
                            dplyr::summarise(
                              min = ifelse(all(is.na(value_as_number)), NA, min(value_as_number, na.rm = TRUE)),
                              percentile_5 = quantile(value_as_number, probs = 0.05, na.rm = TRUE),
                              percentile_25 = quantile(value_as_number, probs = 0.25, na.rm = TRUE),
                              median = median(value_as_number, na.rm = TRUE),
                              mean = mean(value_as_number, na.rm = TRUE),
                              mode = calculate_mode(value_as_number),
                              percentile_75 = quantile(value_as_number, probs = 0.75, na.rm = TRUE),
                              percentile_95 = quantile(value_as_number, probs = 0.95, na.rm = TRUE),
                              max = ifelse(all(is.na(value_as_number)), NA, max(value_as_number, na.rm = TRUE)),
                              standard_deviation = sd(value_as_number, na.rm = TRUE),
                              median_absolute_deviation = mad(value_as_number, na.rm = TRUE),
                              number_of_measurements = dplyr::n(),
                              number_of_patients = dplyr::n_distinct(person_id),
                              number_of_visits = dplyr::n_distinct(visit_occurrence_id),
                              percent_patients = round(dplyr::n_distinct(person_id) / patEncResult$persons[1] * 100, 2),
                              percent_visits = round(dplyr::n_distinct(visit_occurrence_id) / patEncResult$encounters[1] * 100, 2),
                              percent_missing = round(sum(is.na(value_as_number)) / dplyr::n() * 100, 2),
                              min_date = min(measurement_datetime),
                              max_date = max(measurement_datetime)
                            ) %>% dplyr::ungroup(), qData %>%
                            dplyr::group_by(unit_concept_id) %>%
                            dplyr::summarise(
                              min = ifelse(all(is.na(value_as_number)), NA, min(value_as_number, na.rm = TRUE)),
                              percentile_5 = quantile(value_as_number, probs = 0.05, na.rm = TRUE),
                              percentile_25 = quantile(value_as_number, probs = 0.25, na.rm = TRUE),
                              median = median(value_as_number, na.rm = TRUE),
                              mean = mean(value_as_number, na.rm = TRUE),
                              mode = calculate_mode(value_as_number),
                              percentile_75 = quantile(value_as_number, probs = 0.75, na.rm = TRUE),
                              percentile_95 = quantile(value_as_number, probs = 0.95, na.rm = TRUE),
                              max = ifelse(all(is.na(value_as_number)), NA, max(value_as_number, na.rm = TRUE)),
                              standard_deviation = sd(value_as_number, na.rm = TRUE),
                              median_absolute_deviation = mad(value_as_number, na.rm = TRUE),
                              number_of_measurements = dplyr::n(),
                              number_of_patients = dplyr::n_distinct(person_id),
                              number_of_visits = dplyr::n_distinct(visit_occurrence_id),
                              percent_patients = round(dplyr::n_distinct(person_id) / patEncResult$persons[1] * 100, 2),
                              percent_visits = round(dplyr::n_distinct(visit_occurrence_id) / patEncResult$encounters[1] * 100, 2),
                              percent_missing = round(sum(is.na(value_as_number)) / dplyr::n() * 100, 2),
                              min_date = min(measurement_datetime),
                              max_date = max(measurement_datetime)
                            ) %>% dplyr::ungroup() %>%
                            dplyr::mutate(measurement_concept_id=paste(check_name, "overall", sep='_')))

            # calculate value_as_concept stats
            ParallelLogger::logInfo(sprintf("Calculating Value as Concept summary for %s", check_name))
            write.csv(rbind(qData %>%
                            dplyr::group_by(measurement_concept_id, value_as_concept_id) %>%
                            dplyr::summarise(
                              number_of_measurements = dplyr::n(),
                              number_of_patients = dplyr::n_distinct(person_id),
                              number_of_visits = dplyr::n_distinct(visit_occurrence_id),
                              percent_patients = round(dplyr::n_distinct(person_id) / patEncResult$persons[1] * 100, 2),
                              percent_visits = round(dplyr::n_distinct(visit_occurrence_id) / patEncResult$encounters[1] * 100, 2),
                              min_date = min(measurement_datetime),
                              max_date = max(measurement_datetime)
                            ) %>% dplyr::ungroup() %>% dplyr::mutate(percent_missing = NA), qData %>%
                            dplyr::summarise(
                              number_of_measurements = dplyr::n(),
                              number_of_patients = dplyr::n_distinct(person_id),
                              number_of_visits = dplyr::n_distinct(visit_occurrence_id),
                              percent_patients = round(dplyr::n_distinct(person_id) / patEncResult$persons[1] * 100, 2),
                              percent_visits = round(dplyr::n_distinct(visit_occurrence_id) / patEncResult$encounters[1] * 100, 2),
                              percent_missing = round(sum(is.na(value_as_concept_id)) / dplyr::n() * 100, 2),
                              min_date = min(measurement_datetime),
                              max_date = max(measurement_datetime)
                            )%>%
                            dplyr::mutate(measurement_concept_id=paste(check_name, "overall", sep='_'), value_as_concept_id=NA)),
                        paste(baseFilePath, 'value_as_concept_stats.csv', sep='_'), row.names = FALSE)

            if (isTRUE(computeDrift)) {
              ParallelLogger::logInfo(sprintf("Computing data drift for %s", check_name))
              # Isolate drift warnings and errors from the outer tryCatch on
              # .processCheck. Without the local handler, a warning raised
              # inside the drift computation (for example the benign
              # "package built under R x.y.z" notice that some CRAN packages
              # emit on first lazy-load) would trigger the outer warning
              # handler and abort the whole check. Any real error is logged
              # here and swallowed so numeric-stats output for this check is
              # still produced.
              tryCatch(
                withCallingHandlers(
                  .computeDrift(qData = qData, baseFilePath = baseFilePath,
                                minRegimeMonths = minRegimeMonths,
                                maxOriginPoolSize = maxOriginPoolSize,
                                memoryBudgetBytes = driftMemoryBudgetBytes,
                                driftLogLevel = driftLogLevel),
                  warning = function(w) {
                    ParallelLogger::logInfo(sprintf(
                      "Drift (non-fatal) warning for %s: %s", check_name, w$message))
                    invokeRestart("muffleWarning")
                  }
                ),
                error = function(e) {
                  ParallelLogger::logWarn(sprintf(
                    "Drift computation failed for %s: %s",
                    check_name, conditionMessage(e)))
                }
              )
            }
          }
        } else if (grepl('CONCEPT_CENSUS_CHECK', sql, TRUE)){
          # calculate stats
          ParallelLogger::logInfo(sprintf("Calculating Concept summary for %s", check_name))

          if (rowCount == 0) {
            ParallelLogger::logInfo(sprintf("No data found for %s, skipping stats calculation", check_name))
            qStats <- data.frame(
              measurement_concept_id = character(0),
              number_of_measurements = integer(0),
              number_of_patients = integer(0),
              number_of_visits = integer(0),
              percent_patients = numeric(0),
              percent_visits = numeric(0),
              min_date = as.POSIXct(character(0)),
              max_date = as.POSIXct(character(0))
            )
          } else {
            # Compute statistics in R (data already collected from Andromeda)
            qStats <- rbind(qData %>%
                            dplyr::group_by(measurement_concept_id) %>%
                            dplyr::summarise(
                              number_of_measurements = dplyr::n(),
                              number_of_patients = dplyr::n_distinct(person_id),
                              number_of_visits = dplyr::n_distinct(visit_occurrence_id),
                              percent_patients = round(dplyr::n_distinct(person_id) / patEncResult$persons[1] * 100, 2),
                              percent_visits = round(dplyr::n_distinct(visit_occurrence_id) / patEncResult$encounters[1] * 100, 2),
                              min_date = min(measurement_datetime),
                              max_date = max(measurement_datetime)
                            ) %>% dplyr::ungroup(), qData %>%
                            dplyr::summarise(
                              number_of_measurements = dplyr::n(),
                              number_of_patients = dplyr::n_distinct(person_id),
                              number_of_visits = dplyr::n_distinct(visit_occurrence_id),
                              percent_patients = round(dplyr::n_distinct(person_id) / patEncResult$persons[1] * 100, 2),
                              percent_visits = round(dplyr::n_distinct(visit_occurrence_id) / patEncResult$encounters[1] * 100, 2),
                              min_date = min(measurement_datetime),
                              max_date = max(measurement_datetime)
                            ) %>%
                            dplyr::mutate(measurement_concept_id=paste(check_name, "overall", sep='_')))
          }
        }

        # Only save files if we have data
        if (rowCount > 0) {
          # save results
          ParallelLogger::logInfo(sprintf("Saving %s Summary Files", check_name))
          if (grepl('VALUE_AS_CONCEPT_CHECK', sql, TRUE) | grepl('VALUE_AS_NUMBER_CHECK', sql, TRUE) | grepl('CONCEPT_CENSUS_CHECK', sql, TRUE)){
            write.csv(qStats, paste(baseFilePath, 'stats.csv', sep='_'), row.names = FALSE)
          }

          # Monthly time-stats aggregation runs against the Andromeda-backed
          # table rather than the in-memory `qData`. Calling format() on a
          # POSIXct column of the full row count would expand each timestamp
          # into an 11-component POSIXlt, which blows memory on the largest
          # concepts. Pushing the year/month derivation and the aggregation
          # down to the backing store returns only one row per group.
          timeQuery <- andromedaObject$query_result %>%
            dplyr::mutate(
              year = strftime(measurement_datetime, "%Y"),
              month = strftime(measurement_datetime, "%m")
            )

          if (grepl('VALUE_AS_NUMBER_CHECK', sql, TRUE)) {
            ParallelLogger::logInfo(sprintf("Calculating Numeric Time Stats for %s", check_name))
            # Standard deviation is derived from the two-pass formula in
            # terms of sum(x), sum(x*x), and n rather than a built-in `sd()`
            # so the whole aggregation runs in the backing store (whose
            # supported aggregate function set is a subset of R's). The
            # collected result is one row per (concept, unit, year, month).
            numeric_time_stats <- dplyr::bind_rows(
              timeQuery %>%
                dplyr::group_by(measurement_concept_id, unit_concept_id, year, month) %>%
                dplyr::summarise(
                  num_meas = dplyr::n(),
                  min = min(value_as_number, na.rm = TRUE),
                  mean = mean(value_as_number, na.rm = TRUE),
                  max = max(value_as_number, na.rm = TRUE),
                  sum_x = sum(value_as_number, na.rm = TRUE),
                  sum_xx = sum(value_as_number * value_as_number, na.rm = TRUE),
                  n_val = sum(!is.na(value_as_number))
                ) %>%
                dplyr::ungroup() %>%
                dplyr::collect() %>%
                # the overall rows below carry a synthetic character id, and
                # bind_rows() will not combine <double> with <character>
                dplyr::mutate(measurement_concept_id = as.character(measurement_concept_id)),
              timeQuery %>%
                dplyr::group_by(unit_concept_id, year, month) %>%
                dplyr::summarise(
                  num_meas = dplyr::n(),
                  min = min(value_as_number, na.rm = TRUE),
                  mean = mean(value_as_number, na.rm = TRUE),
                  max = max(value_as_number, na.rm = TRUE),
                  sum_x = sum(value_as_number, na.rm = TRUE),
                  sum_xx = sum(value_as_number * value_as_number, na.rm = TRUE),
                  n_val = sum(!is.na(value_as_number))
                ) %>%
                dplyr::ungroup() %>%
                dplyr::collect() %>%
                dplyr::mutate(measurement_concept_id = paste(check_name, "overall", sep = "_"))
            ) %>%
              dplyr::mutate(
                # pmax(., 0) guards against tiny negative values from floating-point
                # cancellation in sum_xx - sum_x^2/n
                standard_deviation = ifelse(
                  n_val > 1,
                  sqrt(pmax((sum_xx - (sum_x * sum_x) / n_val) / (n_val - 1), 0)),
                  NA_real_
                ),
                num_meas = as.integer(num_meas)
              ) %>%
              dplyr::select(measurement_concept_id, unit_concept_id, year, month,
                            num_meas, min, mean, max, standard_deviation) %>%
              dplyr::arrange(measurement_concept_id, unit_concept_id, year, month) %>%
              as.data.frame()

            write.csv(numeric_time_stats, paste(baseFilePath, 'numeric_time_stats.csv', sep = '_'), row.names = FALSE)
          } else if (grepl('CONCEPT_CENSUS_CHECK', sql, TRUE)) {
            ParallelLogger::logInfo(sprintf("Calculating Concept Time Stats for %s", check_name))
            concept_time_stats <- timeQuery %>%
              dplyr::count(measurement_concept_id, year, month, name = "num_meas") %>%
              dplyr::arrange(measurement_concept_id, year, month) %>%
              dplyr::collect() %>%
              dplyr::mutate(num_meas = as.integer(num_meas)) %>%
              as.data.frame()

            write.csv(concept_time_stats, paste(baseFilePath, 'concept_time_stats.csv', sep = '_'), row.names = FALSE)
          }
        }

        # create output to match expected output (match recordResult.R)
        result <- data.frame(
          numViolatedRows = 0,
          pctViolatedRows = 0,
          numDenominatorRows = 1
        )



      } else {
        result <- DatabaseConnector::querySql(
        connection = connection, sql = sql,
        errorReportFile = errorReportFile,
        snakeCaseToCamelCase = TRUE
      )
      }

      delta <- difftime(Sys.time(), start, units = "secs")
      return(.recordResult(
        result = result, check = check, checkDescription = checkDescription, sql = sql,
        executionTime = sprintf("%f %s", delta, attr(delta, "units"))
      ))
    },
    warning = function(w) {
      ParallelLogger::logWarn(sprintf(
        "[Level: %s] [Check: %s] [CDM Table: %s] [CDM Field: %s] %s",
        checkDescription$checkLevel,
        checkDescription$checkName,
        check["cdmTableName"],
        check["cdmFieldName"], w$message
      ))
      return(.recordResult(check = check, checkDescription = checkDescription, sql = sql, warning = w$message))
    },
    error = function(e) {
      errorMessage <- .flattenConditionMessage(e)
      ParallelLogger::logError(sprintf(
        "[Level: %s] [Check: %s] [CDM Table: %s] [CDM Field: %s] %s",
        checkDescription$checkLevel,
        checkDescription$checkName,
        check["cdmTableName"],
        check["cdmFieldName"], errorMessage
      ))
      return(.recordResult(check = check, checkDescription = checkDescription, sql = sql, error = errorMessage))
    }
  )
}
