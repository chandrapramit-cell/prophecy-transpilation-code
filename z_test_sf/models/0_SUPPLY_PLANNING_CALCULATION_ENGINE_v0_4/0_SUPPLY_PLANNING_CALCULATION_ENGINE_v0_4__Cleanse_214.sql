{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DbFileInput_8_8 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'DbFileInput_8_8') }}

),

AlteryxSelect_9 AS (

  {#VisualGroup: INPUTITEMMASTER#}
  SELECT 
    SOURCE_SKU AS SOURCE_SKU,
    SOURCE_SKU_DESC AS SOURCE_SKU_DESC,
    SKU_STANDARD AS SKU_STANDARD,
    SKU_DESC_STANDARD AS SKU_DESC_STANDARD,
    "PRP3 DESC" AS SKU_CATEGORY
  
  FROM DbFileInput_8_8 AS in0

),

Filter_13 AS (

  {#VisualGroup: INPUTITEMMASTER#}
  SELECT * 
  
  FROM AlteryxSelect_9 AS in0
  
  WHERE (NOT(SOURCE_SKU IS NULL))

),

Cleanse_214 AS (

  {#VisualGroup: INPUTITEMMASTER#}
  {{
    prophecy_basics.DataCleansing(
      ['Filter_13'], 
      [
        { "name": "SKU_CATEGORY", "dataType": "String" }, 
        { "name": "SOURCE_SKU", "dataType": "String" }, 
        { "name": "SKU_DESC_STANDARD", "dataType": "String" }, 
        { "name": "SOURCE_SKU_DESC", "dataType": "String" }, 
        { "name": "SKU_STANDARD", "dataType": "String" }
      ], 
      'makeUppercase', 
      ['SOURCE_SKU', 'SOURCE_SKU_DESC', 'SKU_STANDARD', 'SKU_DESC_STANDARD', 'SKU_CATEGORY'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

)

SELECT *

FROM Cleanse_214
