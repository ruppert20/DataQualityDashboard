/*********
CONCEPT LEVEL check:
PLAUSIBLE_VALUE_LOW - find any MEASUREMENT records that have VALUE_AS_NUMBER with non-null value < plausible low value

Parameters used in this template:
schema = @schema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
conceptId = @conceptId
unitConceptId = @unitConceptId
plausibleValueLow = @plausibleValueLow
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
FROM (
	SELECT 
		COUNT_BIG(*) AS num_violated_rows
	FROM
	(
		/*violatedRowsBegin*/
		SELECT 
			m.* 
		FROM @schema.@cdmTableName m
		{@cohort}?{
			JOIN @cohortDatabaseSchema.@cohortTableName c
				ON m.person_id = c.subject_id
				AND c.cohort_definition_id = @cohortDefinitionId
		}
		WHERE m.@cdmFieldName = @conceptId
			AND m.unit_concept_id = @unitConceptId
			AND m.value_as_number IS NOT NULL
			AND m.value_as_number < @plausibleValueLow
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
	FROM @schema.@cdmTableName m
	{@cohort}?{
		JOIN @cohortDatabaseSchema.@cohortTableName c
			ON m.person_id = c.subject_id
			AND c.cohort_definition_id = @cohortDefinitionId
	}
	WHERE m.@cdmFieldName = @conceptId
		AND unit_concept_id = @unitConceptId
		AND value_as_number IS NOT NULL
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
