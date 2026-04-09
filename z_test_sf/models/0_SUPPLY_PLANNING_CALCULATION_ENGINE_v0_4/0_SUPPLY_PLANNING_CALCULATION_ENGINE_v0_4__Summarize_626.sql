{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TargetQuan_xlsx_620 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'TargetQuan_xlsx_620') }}

),

AlteryxSelect_621 AS (

  SELECT 
    SKU_STANDARD AS SKU_STANDARD,
    WH_ID_STANDARD AS WH_ID_STANDARD,
    TARGET_QUAN_OH AS TARGET_QUAN_OH
  
  FROM TargetQuan_xlsx_620 AS in0

),

Filter_622 AS (

  SELECT * 
  
  FROM AlteryxSelect_621 AS in0
  
  WHERE (NOT(SKU_STANDARD IS NULL))

),

Summarize_624 AS (

  SELECT 
    SUM(TARGET_QUAN_OH) AS TARGET_QUAN_OH,
    SKU_STANDARD AS SKU_STANDARD,
    WH_ID_STANDARD AS WH_ID_STANDARD
  
  FROM Filter_622 AS in0
  
  GROUP BY 
    SKU_STANDARD, WH_ID_STANDARD

),

Summarize_626 AS (

  SELECT 
    SUM(TARGET_QUAN_OH) AS TARGET_QUAN_OH,
    SKU_STANDARD AS SKU_STANDARD
  
  FROM Summarize_624 AS in0
  
  GROUP BY SKU_STANDARD

)

SELECT *

FROM Summarize_626
