{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DynamicInput_15 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Log_Extraction', 'DynamicInput_15') }}

),

Filter_16 AS (

  SELECT * 
  
  FROM DynamicInput_15 AS in0
  
  WHERE {{ var('VARIABLE16_EXPRESSION') }}

)

SELECT *

FROM Filter_16
