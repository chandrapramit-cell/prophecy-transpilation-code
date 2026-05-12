{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DynamicInput_17 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Log_Extraction', 'DynamicInput_17') }}

),

Formula_21_0 AS (

  SELECT 
    {{ var('VARIABLE21_FORMULAFIELDS_FORMULAFIELDFIELDDIRECTORY_NEW_EXPRESSION') }} AS DIRECTORY_NEW,
    *
  
  FROM DynamicInput_17 AS in0

),

Formula_21_1 AS (

  SELECT 
    CAST((CONCAT(DIRECTORY_NEW, (REGEXP_SUBSTR(FILENAME, '[^\\]+$')), (REGEXP_SUBSTR(FILENAME, '[^\\]+$')))) AS STRING) AS LOGOUTPUTPATH,
    *
  
  FROM Formula_21_0 AS in0

),

AlteryxSelect_41 AS (

  SELECT 
    FIELD_1 AS FIELD_1,
    DIRECTORY_NEW AS DIRECTORY_NEW,
    LOGOUTPUTPATH AS LOGOUTPUTPATH
  
  FROM Formula_21_1 AS in0

)

SELECT *

FROM AlteryxSelect_41
