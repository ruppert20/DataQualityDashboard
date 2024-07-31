/*********
VALUE_AS_NUMBER_CHECK
PLAUSIBLE_VALUE_HIGH - find any MEASUREMENT records that have VALUE_AS_NUMBER with non-null value > plausible high value

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
}
XXXSAVE_FULL_RESULTXXX
XXXQUERYNAME___VALUE_AS_CONCEPT_CHECKXXX
**********/

SELECT 
	m.person_id,
	m.visit_occurrence_id,
	m.@cdmFieldName,
	m.value_as_concept_id

FROM @cdmDatabaseSchema.@cdmTableName m
	{@cohort}?{
		JOIN @cohortDatabaseSchema.@cohortTableName c
			ON m.person_id = c.subject_id
			AND c.cohort_definition_id = @cohortDefinitionId
	}
WHERE
	m.@cdmFieldName IN (@conceptId)
