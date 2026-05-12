{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Directory_10 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Log_Extraction', 'Directory_10') }}

),

Directory_10_reformat AS (

  SELECT 
    1 AS FULLPATH,
    1 AS DIRECTORY,
    1 AS FILENAME,
    1 AS SHORTFILENAME,
    1 AS CREATIONTIME,
    1 AS LASTACCESSTIME,
    1 AS LASTWRITETIME,
    1 AS SIZE,
    NULL AS ATTRIBUTEARCHIVE,
    NULL AS ATTRIBUTECOMPRESSED,
    NULL AS ATTRIBUTEENCRYPTED,
    NULL AS ATTRIBUTEHIDDEN,
    NULL AS ATTRIBUTEREADONLY,
    NULL AS ATTRIBUTESYSTEM,
    NULL AS ATTRIBUTETEMPORARY
  
  FROM Directory_10 AS in0

),

AlteryxSelect_11 AS (

  SELECT 
    FULLPATH AS FULLPATH,
    CAST((TRY_TO_TIMESTAMP(CAST(LASTWRITETIME AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS DATE) AS LASTWRITETIME
  
  FROM Directory_10_reformat AS in0

),

Filter_12 AS (

  SELECT * 
  
  FROM AlteryxSelect_11 AS in0
  
  WHERE false

),

Formula_35_14_0 AS (

  SELECT 
    CAST('internal' AS STRING) AS INTERNAL_FILEPATH,
    *
  
  FROM Filter_12 AS in0

)

SELECT *

FROM Formula_35_14_0
