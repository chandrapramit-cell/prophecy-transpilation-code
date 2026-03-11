{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_64 AS (

  SELECT CASE
           WHEN not(CAST(not(contains(coalesce(lower({{ var('List_Box_48') }}), ''), lower('True'))) AS BOOLEAN))
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 64')
         END AS check_config64

)

SELECT *

FROM Error_64
