/*********
PATIENT_AND_ENCOUNTER_CHECK

Parameters used in this template:
cdmDatabaseSchema = @cdmDatabaseSchema
cdmTableName = @cdmTableName
cdmFieldName = @cdmFieldName
conceptId = @conceptId
{@cohort}?{
cohortDefinitionId = @cohortDefinitionId
cohortDatabaseSchema = @cohortDatabaseSchema
cohortTableName = @cohortTableName
}
XXXSAVE_FULL_RESULTXXX
XXXQUERYNAME___@PatEncStatsNotesXXX
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
