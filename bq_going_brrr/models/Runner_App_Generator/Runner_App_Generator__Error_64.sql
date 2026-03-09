{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Error_64 AS (

  SELECT CASE
           WHEN not(
             CAST((
               (({{ var('YXMC_Check_Box') }} = 'False') AND ({{ var('YXMD_Check_Box') }} = 'False'))
               AND ({{ var('YXWZ_Check_Box') }} = 'False')
             ) AS BOOLEAN))
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 64')
         END AS check_config64

)

SELECT *

FROM Error_64
