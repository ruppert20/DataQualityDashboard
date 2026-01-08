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

#' Internal function to send the fully qualified sql to the database and return the numerical result.
#'
#' @param connection                A connection for connecting to the CDM database using the DatabaseConnector::connect(connectionDetails) function.
#' @param connectionDetails         A connectionDetails object for connecting to the CDM database.
#' @param check                     The data quality check
#' @param checkDescription          The description of the data quality check
#' @param sql                       The fully qualified sql for the data quality check
#' @param outputFolder              The folder to output logs and SQL files to.
#' @param patEncSql                 The SQL for patient and encounter statistics
#' @param cdmVersion                The CDM version (e.g., "5.3", "5.4")
#' @param resume                    Whether to resume from existing parquet files
#'
#' @return A dataframe containing the check results
#'
#' @keywords internal
#'
calculate_mode <- function(x) {
  tbl <- table(x)
  if (length(tbl) == 0) {
    return(NA)
  }
  modes <- tbl == max(tbl)
  as.numeric(names(modes)[which.max(modes)])
}

.processCheck <- function(connection,
                          connectionDetails,
                          check,
                          checkDescription,
                          sql,
                          outputFolder,
                          patEncSql,
                          cdmVersion = "5.4",
                          resume = TRUE) {
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
     # check if full query needs to be saved
      if (grepl('XXXSAVE_FULL_RESULTXXX', sql, TRUE)) {

        # get the visit and person stats for the selected cohort
        patEncResult <- DatabaseConnector::querySql(
                                                    connection = connection, sql = patEncSql,
                                                    errorReportFile = errorReportFile,
                                                    snakeCaseToCamelCase = TRUE
      )

        # extract Variable name
        check_name <- paste(stringr::str_replace(stringr::str_replace(stringr::str_extract(sql, "XXXQUERYNAME___[A-z_0-9_]+XXX"), "XXX$", ""), "^XXXQUERYNAME___", ""),
                            tolower(check["cdmTableName"]), sep='_')

        # SQL already has correct date columns from CSV configuration (rendered in runCheck.R)
        querySQL <- tolower(sql)

        # define base file path
        baseFilePath <- file.path(outputFolder, check_name)

        parquetDir <- paste(baseFilePath, "parquet", sep='_')
        metadataFile <- file.path(parquetDir, "_metadata")

        if (resume & file.exists(metadataFile)) {
          ParallelLogger::logInfo(sprintf("Resuming %s from parquet", check_name))
        } else {
          # Create/clean parquet directory
          if (dir.exists(parquetDir)) {
            unlink(parquetDir, recursive = TRUE)
          }
          dir.create(parquetDir, recursive = TRUE)

          ParallelLogger::logInfo(sprintf("Running %s Query", check_name))

          # Use database-side pagination to avoid loading entire result into JVM memory
          batch_num <- 0
          batch_size <- 500000  # rows per batch (~250MB assuming ~500 bytes/row)
          total_rows <- 0
          offset <- 0

          # Get target DBMS for SQL translation
          targetDialect <- connectionDetails$dbms

          # Wrap query for pagination - use subquery to allow LIMIT/OFFSET
          # Remove trailing semicolon if present for subquery wrapping
          baseQuery <- sub(";\\s*$", "", querySQL)

          repeat {
            # Build paginated query using SqlRender for cross-database compatibility
            paginatedSql <- sprintf(
              "SELECT * FROM (%s) paginated_query LIMIT %d OFFSET %d;",
              baseQuery, batch_size, offset
            )
            paginatedSql <- SqlRender::translate(paginatedSql, targetDialect = targetDialect)

            batch <- DatabaseConnector::querySql(
              connection = connection,
              sql = paginatedSql,
              snakeCaseToCamelCase = FALSE,
              integer64AsNumeric = FALSE
            )

            if (nrow(batch) > 0) {
              total_rows <- total_rows + nrow(batch)
              arrow::write_parquet(
                batch,
                file.path(parquetDir, sprintf("batch_%04d.parquet", batch_num))
              )
              batch_num <- batch_num + 1
              ParallelLogger::logInfo(sprintf("  Wrote batch %d (%d rows, %d total)", batch_num, nrow(batch), total_rows))

              # If we got fewer rows than batch_size, we've reached the end
              if (nrow(batch) < batch_size) {
                break
              }
              offset <- offset + batch_size
            } else {
              # No rows returned
              if (batch_num == 0) {
                # Write empty parquet file with correct schema on first batch
                arrow::write_parquet(
                  batch,
                  file.path(parquetDir, "batch_0000.parquet")
                )
              }
              break
            }
          }

          # Write _metadata file (standard Parquet dataset marker)
          # Contains unified schema from all files - indicates successful completion
          ds <- arrow::open_dataset(parquetDir)
          arrow::write_parquet(
            arrow::arrow_table(schema = ds$schema),
            metadataFile
          )

          ParallelLogger::logInfo(sprintf("Finished writing %d rows to %d parquet files", total_rows, batch_num))
        }

        # Read parquet dataset (Arrow handles int64 correctly)
        qData <- dplyr::collect(arrow::open_dataset(parquetDir))

        # Convert int64 columns to character for consistent downstream processing
        int64_cols <- c("measurement_concept_id", "person_id", "visit_occurrence_id",
                        "unit_concept_id", "value_as_concept_id")
        for (col in int64_cols) {
          if (col %in% names(qData) && inherits(qData[[col]], "integer64")) {
            qData[[col]] <- as.character(qData[[col]])
          }
        }

        if (grepl('VALUE_AS_NUMBER_CHECK', sql, TRUE)){
          # calculate stats
          ParallelLogger::logInfo(sprintf("Calculating Numeric summary for %s", check_name))

          # Skip stats calculation if no data
          if (nrow(qData) == 0) {
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

            # calculate stats
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
          }
        } else if (grepl('CONCEPT_CENSUS_CHECK', sql, TRUE)){
          # calculate stats
          ParallelLogger::logInfo(sprintf("Calculating Concept summary for %s", check_name))

          # Skip stats calculation if no data
          if (nrow(qData) == 0) {
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
        if (nrow(qData) > 0) {
          # create table of Values over time
          hist_data <- qData %>%
            dplyr::arrange(measurement_datetime) %>%
            dplyr::mutate(month = format(measurement_datetime, "%m"), year = format(measurement_datetime, "%Y")) %>%
            dplyr::group_by(year, month) %>%
            dplyr::mutate(num_meas = dplyr::n()) %>%
            dplyr::distinct(year, month, num_meas) %>%
            dplyr::ungroup() %>%
            dplyr::arrange(year, month) %>%
            dplyr::collect()

          # save results
          ParallelLogger::logInfo(sprintf("Saving %s Summary Files", check_name))
          if (grepl('VALUE_AS_CONCEPT_CHECK', sql, TRUE) | grepl('VALUE_AS_NUMBER_CHECK', sql, TRUE) | grepl('CONCEPT_CENSUS_CHECK', sql, TRUE)){
            write.csv(qStats, paste(baseFilePath, 'stats.csv', sep='_'), row.names = FALSE)
          }

          write.csv(hist_data, paste(baseFilePath, 'time_stats.csv', sep='_'), row.names = FALSE)
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
      ParallelLogger::logError(sprintf(
        "[Level: %s] [Check: %s] [CDM Table: %s] [CDM Field: %s] %s",
        checkDescription$checkLevel,
        checkDescription$checkName,
        check["cdmTableName"],
        check["cdmFieldName"], e$message
      ))
      return(.recordResult(check = check, checkDescription = checkDescription, sql = sql, error = e$message))
    }
  )
}
