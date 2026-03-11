{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_59 AS (

  SELECT CASE
           WHEN not(
             CAST((
               coalesce(
                 CAST(substring({{ var('Text_Box_42') }}, 1, 4) AS DOUBLE), 
                 CAST(regexp_extract(substring({{ var('Text_Box_42') }}, 1, 4), '^[0-9]+', 0) AS INT), 
                 0) < 1958
             ) AS BOOLEAN))
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 59')
         END AS check_config59

)

SELECT *

FROM Error_59
