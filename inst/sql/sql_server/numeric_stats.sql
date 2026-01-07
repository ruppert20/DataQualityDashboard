/*********
VALUE_AS_NUMBER_CHECK

Parameters used in this template:
cdmDatabaseSchema = @cdmDatabaseSchema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
conceptId = @conceptId
unitConceptId = @unitConceptId
datetimeColumn = @datetimeColumn
dateColumn = @dateColumn
endDateColumn = @endDateColumn (optional)
cohortFilterType = @cohortFilterType (PersonOnly, PersonDate, PersonDateTime)
cohortHasDatetime = @cohortHasDatetime (TRUE if cohort table has datetime columns)
XXXSAVE_FULL_RESULTXXX
XXXQUERYNAME___@conceptNameXXX
{@cohort}?{
cohortDefinitionId = @cohortDefinitionId
cohortDatabaseSchema = @cohortDatabaseSchema
cohortTableName = @cohortTableName
}
additionalSQLFilters = @additionalSQLFilters (optional, additional WHERE clause conditions)
**********/

SELECT
	m.person_id,
	m.visit_occurrence_id,
	m.@datetimeColumn as measurement_datetime,
	m.@cdmFieldName as measurement_concept_id,
	m.value_as_concept_id,
	m.value_as_number,
	m.unit_concept_id

FROM @cdmDatabaseSchema.@cdmTableName m
	{@cohort}?{
		JOIN @cohortDatabaseSchema.@cohortTableName c
			ON m.person_id = c.subject_id
			AND c.cohort_definition_id = @cohortDefinitionId
	}
WHERE
	m.@cdmFieldName IN (@conceptId)
	{@unitConceptId NOT IN ('NA', -1, FALSE, '')}?{
		AND m.unit_concept_id IN (@unitConceptId)
	}
	-- PersonDate filter: use date columns, intersection logic (COALESCE handles NULL end dates)
	{@cohort & @cohortFilterType == 'PersonDate' & @endDateColumn != ''}?{
		AND m.@dateColumn <= c.cohort_end_date
		AND COALESCE(m.@endDateColumn, m.@dateColumn) >= c.cohort_start_date
	}
	{@cohort & @cohortFilterType == 'PersonDate' & @endDateColumn == ''}?{
		AND m.@dateColumn >= c.cohort_start_date
		AND m.@dateColumn <= c.cohort_end_date
	}
	-- PersonDateTime filter with cohort datetime columns available
	{@cohort & @cohortFilterType == 'PersonDateTime' & @cohortHasDatetime & @endDateColumn != ''}?{
		AND m.@datetimeColumn <= c.cohort_end_datetime
		AND COALESCE(m.@endDateColumn, m.@dateColumn) >= c.cohort_start_datetime
	}
	{@cohort & @cohortFilterType == 'PersonDateTime' & @cohortHasDatetime & @endDateColumn == ''}?{
		AND m.@datetimeColumn >= c.cohort_start_datetime
		AND m.@datetimeColumn <= c.cohort_end_datetime
	}
	-- PersonDateTime filter fallback to date columns (cast datetime to date)
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
	};
