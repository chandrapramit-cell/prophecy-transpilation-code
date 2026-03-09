{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Error_79 AS (

  SELECT CASE
           WHEN not(
             CAST((
               (isnull({{ var('FolderSelection') }}) OR isnull(trim({{ var('FolderSelection') }})))
               OR (length(trim({{ var('FolderSelection') }})) = 0)
             ) AS BOOLEAN))
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 79')
         END AS check_config79

)

SELECT *

FROM Error_79
