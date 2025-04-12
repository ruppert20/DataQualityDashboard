# Copyright 2024 Observational Health Data Sciences and Informatics
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
#' @param resume                    Whether to used a cached andromeda query of the result or to fetch a new one from the server. This paramter is for developer purposes only and is not intented to be used in production.
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
        

        # extract Variable name
        check_name <- paste(stringr::str_replace(stringr::str_replace(stringr::str_extract(sql, "XXXQUERYNAME___[A-z_0-9_]+XXX"), "XXX$", ""), "^XXXQUERYNAME___", ""),
                            tolower(check["cdmTableName"]), sep='_')

        # replace any bad timecolumns
        querySQL <- gsub("exposure_datetime", "exposure_start_datetime", tolower(sql))
        querySQL <- gsub("procedure_occurrence_datetime", "procedure_date", querySQL)
        querySQL <- gsub("occurrence_datetime", "start_datetime", querySQL)
        querySQL <- gsub("condition_start_datetime", "condition_start_date", querySQL)

        # define base file path
        baseFilePath <- file.path(outputFolder, check_name)

        andromedaFP <- paste(baseFilePath, "andromeda", sep='.')

        if (resume & file.exists(andromedaFP)) {
          andromedaObject <- Andromeda::loadAndromeda(andromedaFP)
        } else {
          # create andromeda object
          andromedaObject <- Andromeda::andromeda()

          ParallelLogger::logInfo(sprintf("Running %s Query", check_name))
          # save query result to andromeda object
          DatabaseConnector::querySqlToAndromeda(
            connection = connection,
            sql = querySQL,
            andromeda = andromedaObject,
            andromedaTableName = 'query_result',
            errorReportFile = errorReportFile,
            snakeCaseToCamelCase = FALSE,
            appendToTable = FALSE,
            integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
            integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE)
          )

          # save andromeda object
          Andromeda::saveAndromeda(andromeda = andromedaObject,
                                  fileName = andromedaFP,
                                  maintainConnection = TRUE,
                                  overwrite = TRUE)
        }

        

        # query the andromeda database to get required columns for statistics
        qData <- RSQLite::dbGetQuery(andromedaObject, "SELECT * FROM query_result;")

        if (grepl('VALUE_AS_NUMBER_CHECK', sql, TRUE)){
          # calculate stats
          ParallelLogger::logInfo(sprintf("Calculating Numeric summary for %s", check_name))
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
                            min_date = min(measurement_datetime),
                            max_date = max(measurement_datetime)
                          ) %>% dplyr::ungroup() %>% dplyr::mutate(percent_missing = NA), qData %>%
                          dplyr::summarise(
                            number_of_measurements = dplyr::n(),
                            number_of_patients = dplyr::n_distinct(person_id),
                            number_of_visits = dplyr::n_distinct(visit_occurrence_id),
                            percent_missing = round(sum(is.na(value_as_concept_id)) / dplyr::n() * 100, 2),
                            min_date = min(measurement_datetime),
                            max_date = max(measurement_datetime)
                          )%>%
                          dplyr::mutate(measurement_concept_id=paste(check_name, "overall", sep='_'), value_as_concept_id=NA)),
                          paste(baseFilePath, 'value_as_concept_stats.csv', sep='_'), row.names = FALSE)
        } else if (grepl('CONCEPT_CENSUS_CHECK', sql, TRUE)){
           # calculate stats
          ParallelLogger::logInfo(sprintf("Calculating Concept summary for %s", check_name))
          qStats <- rbind(qData %>% 
                          dplyr::group_by(measurement_concept_id) %>%
                          dplyr::summarise(
                            number_of_measurements = dplyr::n(),
                            number_of_patients = dplyr::n_distinct(person_id),
                            number_of_visits = dplyr::n_distinct(visit_occurrence_id),
                            min_date = min(measurement_datetime),
                            max_date = max(measurement_datetime)
                          ) %>% dplyr::ungroup(), qData %>%
                          dplyr::summarise(
                            number_of_measurements = dplyr::n(),
                            number_of_patients = dplyr::n_distinct(person_id),
                            number_of_visits = dplyr::n_distinct(visit_occurrence_id),
                            min_date = min(measurement_datetime),
                            max_date = max(measurement_datetime)
                          ) %>%
                          dplyr::mutate(measurement_concept_id=paste(check_name, "overall", sep='_')))
        }
        
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

        # close andromeda object
        Andromeda::close(andromedaObject)

        # create output to match expected output
        result <-  data.frame(
          num_violated_rows= c(0), 
          pct_violated_rows = c(0),
          num_denominator_rows = c(1)
        )


          
      } else {
        result <- DatabaseConnector::querySql(
        connection = connection, sql = sql,
        errorReportFile = errorReportFile,
        snakeCaseToCamelCase = TRUE
      )
      }
      

      delta <- difftime(Sys.time(), start, units = "secs")
      .recordResult(
        result = result, check = check, checkDescription = checkDescription, sql = sql,
        executionTime = sprintf("%f %s", delta, attr(delta, "units"))
      )
    },
    warning = function(w) {
      ParallelLogger::logWarn(sprintf(
        "[Level: %s] [Check: %s] [CDM Table: %s] [CDM Field: %s] %s",
        checkDescription$checkLevel,
        checkDescription$checkName,
        check["cdmTableName"],
        check["cdmFieldName"], w$message
      ))
      .recordResult(check = check, checkDescription = checkDescription, sql = sql, warning = w$message)
    },
    error = function(e) {
      ParallelLogger::logError(sprintf(
        "[Level: %s] [Check: %s] [CDM Table: %s] [CDM Field: %s] %s",
        checkDescription$checkLevel,
        checkDescription$checkName,
        check["cdmTableName"],
        check["cdmFieldName"], e$message
      ))
      .recordResult(check = check, checkDescription = checkDescription, sql = sql, error = e$message)
    }
  )
}
