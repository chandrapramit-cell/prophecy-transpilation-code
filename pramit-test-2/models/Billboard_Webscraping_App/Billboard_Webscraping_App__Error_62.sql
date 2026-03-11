{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_62 AS (

  SELECT CASE
           WHEN (NOT CAST((CAST(DATE_DIFF(CAST(CAST(CURRENT_DATE AS string) AS DATE), CAST({{ var('TEXT_BOX_42') }} AS DATE), DAY) AS INT64) < 0) AS BOOLEAN))
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 62')
         END AS check_config62

)

SELECT *

FROM Error_62
