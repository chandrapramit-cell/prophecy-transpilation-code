{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_62 AS (

  SELECT CASE
           WHEN not(
             CAST((CAST(datediff(to_date(current_date()), to_date({{ var('Text_Box_42') }})) AS INT) < 0) AS BOOLEAN))
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 62')
         END AS check_config62

)

SELECT *

FROM Error_62
