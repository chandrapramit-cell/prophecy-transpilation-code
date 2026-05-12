{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Directory_11_14 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Log_Extraction', 'Directory_11_14') }}

),

Directory_11_14_reformat AS (

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
  
  FROM Directory_11_14 AS in0

),

Formula_35_14_0 AS (

  SELECT *
  
  FROM {{ ref('Log_Extraction__Formula_35_14_0')}}

),

AlteryxSelect_30_14 AS (

  SELECT INTERNAL_FILEPATH AS FULLPATH
  
  FROM Formula_35_14_0 AS in0

),

AlteryxSelect_13_14 AS (

  SELECT FULLPATH AS FULLPATH
  
  FROM Directory_11_14_reformat AS in0

),

Union_12_14 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_30_14', 'AlteryxSelect_13_14'], 
      ['[{"name": "FULLPATH", "dataType": "String"}]', '[{"name": "FULLPATH", "dataType": "Number"}]'], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_12_14
