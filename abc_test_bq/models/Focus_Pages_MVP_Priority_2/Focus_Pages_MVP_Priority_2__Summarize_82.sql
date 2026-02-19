{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_80_1 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Formula_80_1')}}

),

Summarize_82 AS (

  SELECT 
    COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY,
    `Time Since First Diagnosis` AS `Time Since First Diagnosis`,
    `Time Since First Diagnosis paranthesesOpenCatparanthesesClose` AS `Time Since First Diagnosis paranthesesOpenCatparanthesesClose`
  
  FROM Formula_80_1 AS in0
  
  GROUP BY 
    `Time Since First Diagnosis`, `Time Since First Diagnosis paranthesesOpenCatparanthesesClose`

)

SELECT *

FROM Summarize_82
