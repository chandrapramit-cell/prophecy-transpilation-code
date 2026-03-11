{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_58 AS (

  SELECT CASE
           WHEN (
             NOT CAST((
               NOT(
                 (LENGTH((REGEXP_EXTRACT({{ var('TEXT_BOX_42') }}, '\\d{4}-\\d{2}-\\d{2}', 0)))) > 0)
             ) AS BOOLEAN)
           )
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 58')
         END AS check_config58

)

SELECT *

FROM Error_58
