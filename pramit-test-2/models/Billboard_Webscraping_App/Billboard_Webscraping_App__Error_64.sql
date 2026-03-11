{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_64 AS (

  SELECT CASE
           WHEN (NOT CAST((NOT CAST(((STRPOS((coalesce(LOWER({{ var('LIST_BOX_48') }}), '')), LOWER('True'))) > 0) AS BOOLEAN)) AS BOOLEAN))
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 64')
         END AS check_config64

)

SELECT *

FROM Error_64
