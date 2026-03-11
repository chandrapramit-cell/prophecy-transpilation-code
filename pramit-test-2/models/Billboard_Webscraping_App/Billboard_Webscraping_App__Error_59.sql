{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_59 AS (

  SELECT CASE
           WHEN (
             NOT CAST((
               (
                 coalesce(
                   CAST((SUBSTRING({{ var('TEXT_BOX_42') }}, 1, 4)) AS FLOAT64), 
                   CAST((REGEXP_EXTRACT((SUBSTRING({{ var('TEXT_BOX_42') }}, 1, 4)), '^[0-9]+', 0)) AS INT64), 
                   0)
               ) < 1958
             ) AS BOOLEAN)
           )
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 59')
         END AS check_config59

)

SELECT *

FROM Error_59
