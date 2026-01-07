/*********
PATIENT_AND_ENCOUNTER_CHECK

Parameters used in this template:
cdmDatabaseSchema = @cdmDatabaseSchema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
conceptId = @conceptId
cohortFilterType = @cohortFilterType (PersonOnly, PersonDate, PersonDateTime)
cohortHasDatetime = @cohortHasDatetime (TRUE if cohort table has datetime columns)
{@cohort}?{
cohortDefinitionId = @cohortDefinitionId
cohortDatabaseSchema = @cohortDatabaseSchema
cohortTableName = @cohortTableName
}
XXXSAVE_FULL_RESULTXXX
XXXQUERYNAME___@PatEncStatsXXX
**********/

SELECT
	COUNT(DISTINCT m.person_id) as persons,
	COUNT(DISTINCT m.visit_occurrence_id) as encounters

FROM @cdmDatabaseSchema.VISIT_OCCURRENCE m
	{@cohort}?{
		JOIN @cohortDatabaseSchema.@cohortTableName c
			ON m.person_id = c.subject_id
			AND c.cohort_definition_id = @cohortDefinitionId
	}
{@cohort & @cohortFilterType != 'PersonOnly'}?{
WHERE 1=1
	-- PersonDate filter: intersection logic for visit occurrence (COALESCE handles NULL end dates)
	{@cohortFilterType == 'PersonDate'}?{
		AND m.visit_start_date <= c.cohort_end_date
		AND COALESCE(m.visit_end_date, m.visit_start_date) >= c.cohort_start_date
	}
	-- PersonDateTime filter with cohort datetime columns available
	{@cohortFilterType == 'PersonDateTime' & @cohortHasDatetime}?{
		AND m.visit_start_datetime <= c.cohort_end_datetime
		AND COALESCE(m.visit_end_datetime, m.visit_start_datetime) >= c.cohort_start_datetime
	}
	-- PersonDateTime filter fallback to date columns
	{@cohortFilterType == 'PersonDateTime' & !@cohortHasDatetime}?{
		AND m.visit_start_date <= c.cohort_end_date
		AND COALESCE(m.visit_end_date, m.visit_start_date) >= c.cohort_start_date
	}
}
