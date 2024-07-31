/*********
VALUE_AS_NUMBER_CHECK

Parameters used in this template:
cdmDatabaseSchema = @cdmDatabaseSchema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
conceptId = @conceptId
unitConceptId = @unitConceptId
{@cohort}?{
cohortDefinitionId = @cohortDefinitionId
cohortDatabaseSchema = @cohortDatabaseSchema
cohortTableName = @cohortTableName
XXXSAVE_FULL_RESULTXXX
XXXQUERYNAME___VALUE_AS_NUMBER_CHECKXXX
}
**********/

SELECT 
	m.person_id,
	m.visit_occurrence_id,
	m.@cdmFieldName,
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
	{@unitConceptId NOT IN ('NA', -1, False, '')}?{
		AND m.unit_concept_id = @unitConceptId
		};
