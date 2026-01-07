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

#' Get date column configuration for a CDM table
#'
#' @param cdmTableName  The name of the CDM table (case-insensitive)
#' @param cdmVersion    The CDM version (e.g., "5.3", "5.4")
#'
#' @return A list with dateColumn, datetimeColumn, endDateColumn, endDatetimeColumn, and hasEndDate
#'
#' @keywords internal
#'
.getDateColumns <- function(cdmTableName, cdmVersion = "5.4") {
  # Normalize table name to lowercase

  cdmTableName <- tolower(cdmTableName)

  # Build path to date columns CSV
  csvFile <- system.file(
    "csv",
    sprintf("OMOP_CDMv%s_Date_Columns.csv", cdmVersion),
    package = "DataQualityDashboard"
  )

  if (csvFile == "" || !file.exists(csvFile)) {
    stop(sprintf("Date columns configuration not found for CDM version %s", cdmVersion))
  }

  # Read the configuration
  dateConfig <- readr::read_csv(csvFile, show_col_types = FALSE)

  # Find the table (case-insensitive)
  tableRow <- dateConfig[tolower(dateConfig$cdmTableName) == cdmTableName, ]

  if (nrow(tableRow) == 0) {
    # Return NAs if table not found (caller can handle)
    return(list(
      cdmTableName = cdmTableName,
      dateColumn = NA_character_,
      datetimeColumn = NA_character_,
      endDateColumn = NA_character_,
      endDatetimeColumn = NA_character_,
      hasEndDate = FALSE
    ))
  }

  # Extract values, converting empty strings to NA
  dateColumn <- tableRow$dateColumn[1]
  datetimeColumn <- tableRow$datetimeColumn[1]
  endDateColumn <- tableRow$endDateColumn[1]
  endDatetimeColumn <- tableRow$endDatetimeColumn[1]

  # Handle empty strings
  if (!is.na(dateColumn) && dateColumn == "") dateColumn <- NA_character_
  if (!is.na(datetimeColumn) && datetimeColumn == "") datetimeColumn <- NA_character_
  if (!is.na(endDateColumn) && endDateColumn == "") endDateColumn <- NA_character_
  if (!is.na(endDatetimeColumn) && endDatetimeColumn == "") endDatetimeColumn <- NA_character_

  list(
    cdmTableName = cdmTableName,
    dateColumn = dateColumn,
    datetimeColumn = datetimeColumn,
    endDateColumn = endDateColumn,
    endDatetimeColumn = endDatetimeColumn,
    hasEndDate = !is.na(endDateColumn)
  )
}

#' Get the preferred datetime column for a CDM table
#'
#' Returns the datetime column if available, otherwise the date column.
#'
#' @param cdmTableName  The name of the CDM table (case-insensitive)
#' @param cdmVersion    The CDM version (e.g., "5.3", "5.4")
#' @param preferDatetime Whether to prefer datetime over date columns (default TRUE)
#'
#' @return The column name to use for datetime filtering
#'
#' @keywords internal
#'
.getPreferredDatetimeColumn <- function(cdmTableName, cdmVersion = "5.4", preferDatetime = TRUE) {
  cols <- .getDateColumns(cdmTableName, cdmVersion)

  if (preferDatetime && !is.na(cols$datetimeColumn)) {
    return(cols$datetimeColumn)
  }

  if (!is.na(cols$dateColumn)) {
    return(cols$dateColumn)
  }

  # Fallback - should not happen for valid CDM tables
  NA_character_
}
