{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Filter_631 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Filter_631')}}

),

Formula_486_0 AS (

  SELECT 
    CAST(NULL AS STRING) AS v_TZAC,
    *
  
  FROM Filter_631 AS in0

),

Formula_249_to_Formula_258_2 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_249_to_Formula_258_2')}}

),

AlteryxSelect_553 AS (

  SELECT 
    v_Reinsurer AS v_Reinsurer,
    v_TZAC AS v_TZAC
  
  FROM Formula_249_to_Formula_258_2 AS in0

),

Formula_8_to_Formula_23_1 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_8_to_Formula_23_1')}}

),

Formula_99_to_Formula_183_2 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_99_to_Formula_183_2')}}

),

AlteryxSelect_554 AS (

  SELECT 
    v_Reinsurer AS v_Reinsurer,
    v_TZAC AS v_TZAC
  
  FROM Formula_99_to_Formula_183_2 AS in0

),

AlteryxSelect_555 AS (

  SELECT 
    v_Reinsurer AS v_Reinsurer,
    v_TZAC AS v_TZAC
  
  FROM Formula_8_to_Formula_23_1 AS in0

),

Union_482 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_555', 'AlteryxSelect_554', 'Formula_486_0', 'AlteryxSelect_553'], 
      [
        '[{"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_TZAC", "dataType": "String"}]', 
        '[{"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_TZAC", "dataType": "String"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_Period", "dataType": "String"}, {"name": "v_Company", "dataType": "String"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "RUP", "dataType": "Double"}, {"name": "RUA", "dataType": "Double"}]', 
        '[{"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_TZAC", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_482
