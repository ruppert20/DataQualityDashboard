/*********
CONCEPT LEVEL check:
PLAUSIBLE_UNIT_CONCEPT_IDS - find any MEASUREMENT records that are associated with an incorrect UNIT_CONCEPT_ID

Parameters used in this template:
cdmDatabaseSchema = @cdmDatabaseSchema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
conceptId = @conceptId
plausibleUnitConceptIds = @plausibleUnitConceptIds
dateColumn = @dateColumn
datetimeColumn = @datetimeColumn
endDateColumn = @endDateColumn (optional)
cohortFilterType = @cohortFilterType (PersonOnly, PersonDate, PersonDateTime)
cohortHasDatetime = @cohortHasDatetime (TRUE if cohort table has datetime columns)
{@cohort}?{
cohortDefinitionId = @cohortDefinitionId
cohortDatabaseSchema = @cohortDatabaseSchema
cohortTableName = @cohortTableName
}
additionalSQLFilters = @additionalSQLFilters (optional, additional WHERE clause conditions)
**********/


SELECT 
  num_violated_rows, 
	CASE 
		WHEN denominator.num_rows = 0 THEN 0 
		ELSE 1.0*num_violated_rows/denominator.num_rows 
	END AS pct_violated_rows, 
	denominator.num_rows AS num_denominator_rows
FROM
(
	SELECT 
	  COUNT_BIG(*) AS num_violated_rows
	FROM
	(
		/*violatedRowsBegin*/
		SELECT 
		  m.* 
		FROM @cdmDatabaseSchema.@cdmTableName m
  		{@cohort}?{
        JOIN @cohortDatabaseSchema.@cohortTableName c
    		ON m.person_id = c.subject_id
    		AND c.cohort_definition_id = @cohortDefinitionId
    	}
		WHERE m.@cdmFieldName = @conceptId
			AND m.unit_concept_id IS NOT NULL
			/* '-1' stands for the cases when the only plausible unit_concept_id is no unit; 0 prevents flagging rows with a unit_concept_id of 0, which are checked in standardConceptRecordCompleteness */
			AND (
				('@plausibleUnitConceptIds' = '-1' AND m.unit_concept_id != 0)
				OR m.unit_concept_id NOT IN (@plausibleUnitConceptIds, 0)
			)
			{@cohort & @cohortFilterType == 'PersonDate' & @endDateColumn != ''}?{
				AND m.@dateColumn <= c.cohort_end_date
				AND COALESCE(m.@endDateColumn, m.@dateColumn) >= c.cohort_start_date
			}
			{@cohort & @cohortFilterType == 'PersonDate' & @endDateColumn == ''}?{
				AND m.@dateColumn >= c.cohort_start_date
				AND m.@dateColumn <= c.cohort_end_date
			}
			{@cohort & @cohortFilterType == 'PersonDateTime' & @cohortHasDatetime & @endDateColumn != ''}?{
				AND m.@datetimeColumn <= c.cohort_end_datetime
				AND COALESCE(m.@endDateColumn, m.@dateColumn) >= c.cohort_start_datetime
			}
			{@cohort & @cohortFilterType == 'PersonDateTime' & @cohortHasDatetime & @endDateColumn == ''}?{
				AND m.@datetimeColumn >= c.cohort_start_datetime
				AND m.@datetimeColumn <= c.cohort_end_datetime
			}
			{@cohort & @cohortFilterType == 'PersonDateTime' & !@cohortHasDatetime & @endDateColumn != ''}?{
				AND CAST(m.@datetimeColumn AS DATE) <= c.cohort_end_date
				AND COALESCE(m.@endDateColumn, CAST(m.@datetimeColumn AS DATE)) >= c.cohort_start_date
			}
			{@cohort & @cohortFilterType == 'PersonDateTime' & !@cohortHasDatetime & @endDateColumn == ''}?{
				AND CAST(m.@datetimeColumn AS DATE) >= c.cohort_start_date
				AND CAST(m.@datetimeColumn AS DATE) <= c.cohort_end_date
			}
			{@additionalSQLFilters != '' & @additionalSQLFilters != 'NA'}?{
				AND m.@additionalSQLFilters
			}
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT 
	  COUNT_BIG(*) AS num_rows
	FROM @cdmDatabaseSchema.@cdmTableName m
  	{@cohort}?{
    	JOIN @cohortDatabaseSchema.@cohortTableName c
    		ON m.person_id = c.subject_id
    		AND c.cohort_definition_id = @cohortDefinitionId
  	}
	WHERE m.@cdmFieldName = @conceptId
		AND (unit_concept_id != 0 OR unit_concept_id IS NULL)
		{@cohort & @cohortFilterType == 'PersonDate' & @endDateColumn != ''}?{
			AND m.@dateColumn <= c.cohort_end_date
			AND COALESCE(m.@endDateColumn, m.@dateColumn) >= c.cohort_start_date
		}
		{@cohort & @cohortFilterType == 'PersonDate' & @endDateColumn == ''}?{
			AND m.@dateColumn >= c.cohort_start_date
			AND m.@dateColumn <= c.cohort_end_date
		}
		{@cohort & @cohortFilterType == 'PersonDateTime' & @cohortHasDatetime & @endDateColumn != ''}?{
			AND m.@datetimeColumn <= c.cohort_end_datetime
			AND COALESCE(m.@endDateColumn, m.@dateColumn) >= c.cohort_start_datetime
		}
		{@cohort & @cohortFilterType == 'PersonDateTime' & @cohortHasDatetime & @endDateColumn == ''}?{
			AND m.@datetimeColumn >= c.cohort_start_datetime
			AND m.@datetimeColumn <= c.cohort_end_datetime
		}
		{@cohort & @cohortFilterType == 'PersonDateTime' & !@cohortHasDatetime & @endDateColumn != ''}?{
			AND CAST(m.@datetimeColumn AS DATE) <= c.cohort_end_date
			AND COALESCE(m.@endDateColumn, CAST(m.@datetimeColumn AS DATE)) >= c.cohort_start_date
		}
		{@cohort & @cohortFilterType == 'PersonDateTime' & !@cohortHasDatetime & @endDateColumn == ''}?{
			AND CAST(m.@datetimeColumn AS DATE) >= c.cohort_start_date
			AND CAST(m.@datetimeColumn AS DATE) <= c.cohort_end_date
		}
		{@additionalSQLFilters != '' & @additionalSQLFilters != 'NA'}?{
			AND m.@additionalSQLFilters
		}
) denominator
;
