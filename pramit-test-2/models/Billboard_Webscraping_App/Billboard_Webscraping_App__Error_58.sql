{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_58 AS (

  SELECT CASE
           WHEN not(
             CAST(not((length(regexp_extract({{ var('Text_Box_42') }}, '\\d{4}-\\d{2}-\\d{2}', 0)) > 0)) AS BOOLEAN))
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 58')
         END AS check_config58

)

SELECT *

FROM Error_58
