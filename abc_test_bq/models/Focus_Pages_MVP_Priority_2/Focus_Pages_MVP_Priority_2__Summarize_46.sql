{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sort_39 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Sort_39')}}

),

Summarize_45 AS (

  SELECT 
    COUNT(DISTINCT `Diabetes Type`) AS `CountDistinct_Diabetes Type`,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM Sort_39 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

Summarize_46 AS (

  SELECT 
    COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY,
    `CountDistinct_Diabetes Type` AS `CountDistinct_Diabetes Type`
  
  FROM Summarize_45 AS in0
  
  GROUP BY `CountDistinct_Diabetes Type`

)

SELECT *

FROM Summarize_46
