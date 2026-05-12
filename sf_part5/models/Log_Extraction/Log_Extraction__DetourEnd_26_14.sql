{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH JupyterCode_4_14 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Log_Extraction', 'JupyterCode_4_14') }}

),

Detour_25_14_out1 AS (

  SELECT * 
  
  FROM JupyterCode_4_14 AS in0
  
  WHERE TRUE

),

Detour_25_14_out0 AS (

  SELECT * 
  
  FROM JupyterCode_4_14 AS in0
  
  WHERE FALSE

),

AlteryxSelect_6_14 AS (

  SELECT 
    CAST(NULL AS STRING) AS FULLPATH,
    CAST(NULL AS STRING) AS SUCCESS
  
  FROM Detour_25_14_out0 AS in0

),

Formula_35_14_0 AS (

  SELECT *
  
  FROM {{ ref('Log_Extraction__Formula_35_14_0')}}

),

Join_27_14_inner AS (

  SELECT 
    in1.FULLPATH AS FULLPATH,
    in0.*,
    in1.* EXCLUDE ("FULLPATH", "INTERNAL_FILEPATH")
  
  FROM Detour_25_14_out1 AS in0
  INNER JOIN Formula_35_14_0 AS in1
     ON (in0.FULLPATH = in1.INTERNAL_FILEPATH)

),

DetourEnd_26_14 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_6_14', 'Join_27_14_inner'], 
      [
        '[{"name": "FULLPATH", "dataType": "String"}, {"name": "SUCCESS", "dataType": "Boolean"}]', 
        '[{"name": "\\"0\\"", "dataType": "String"}, {"name": "LASTWRITETIME", "dataType": "Date"}, {"name": "FULLPATH", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM DetourEnd_26_14
