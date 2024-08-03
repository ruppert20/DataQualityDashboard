/*********
VALUE_AS_NUMBER_CHECK

Parameters used in this template:
cdmDatabaseSchema = @cdmDatabaseSchema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
conceptId = @conceptId
unitConceptId = @unitConceptId
XXXSAVE_FULL_RESULTXXX
XXXQUERYNAME___@numericStatsNotesXXX
{@cohort}?{
cohortDefinitionId = @cohortDefinitionId
cohortDatabaseSchema = @cohortDatabaseSchema
cohortTableName = @cohortTableName
}
**********/

SELECT 
	m.person_id,
	m.visit_occurrence_id,
	m.@cdmTableName_datetime as measurement_datetime,
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
		};
