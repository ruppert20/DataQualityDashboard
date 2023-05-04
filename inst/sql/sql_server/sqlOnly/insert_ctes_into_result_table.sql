/*********
SQL to insert individual DQD results directly into output table, rather than waiting until collecting all results.
Note that this  does not include information about SQL errors or performance

Parameters used in this template:
queryText
resultsDatabaseSchema
tableName
**********/

WITH cte_all AS (
  @queryText
)
INSERT INTO @resultsDatabaseSchema.@tableName
SELECT *
FROM cte_all
;