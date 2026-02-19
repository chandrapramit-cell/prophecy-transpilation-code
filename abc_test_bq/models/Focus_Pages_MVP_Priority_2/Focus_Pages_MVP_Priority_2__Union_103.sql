{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_93 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Filter_93')}}

),

Formula_104_0 AS (

  SELECT 
    'First Trimester' AS Trimester,
    1 AS Trimester_INT,
    *
  
  FROM Filter_93 AS in0

),

Filter_155 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Filter_155')}}

),

Formula_101_1 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Formula_101_1')}}

),

Union_103 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_101_1', 'Formula_104_0', 'Filter_155'], 
      [
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "SUB_ID", "dataType": "String"}, {"name": "ServiceDate", "dataType": "Date"}, {"name": "MBR_SK", "dataType": "Integer"}, {"name": "SRC_SYS_CD", "dataType": "String"}, {"name": "CLM_ID", "dataType": "String"}, {"name": "PregnancyDiagDesc", "dataType": "String"}, {"name": "Trimester_INT", "dataType": "Integer"}, {"name": "PregnancyDiag", "dataType": "String"}, {"name": "CLM_SVC_STRT_DT_SK", "dataType": "Timestamp"}, {"name": "MBR_ID", "dataType": "String"}, {"name": "Trimester", "dataType": "String"}]', 
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "SUB_ID", "dataType": "String"}, {"name": "ServiceDate", "dataType": "Date"}, {"name": "MBR_SK", "dataType": "Integer"}, {"name": "SRC_SYS_CD", "dataType": "String"}, {"name": "CLM_ID", "dataType": "String"}, {"name": "PregnancyDiagDesc", "dataType": "String"}, {"name": "Trimester_INT", "dataType": "Integer"}, {"name": "PregnancyDiag", "dataType": "String"}, {"name": "CLM_SVC_STRT_DT_SK", "dataType": "Timestamp"}, {"name": "MBR_ID", "dataType": "String"}, {"name": "Trimester", "dataType": "String"}]', 
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "SUB_ID", "dataType": "String"}, {"name": "ServiceDate", "dataType": "Date"}, {"name": "MBR_SK", "dataType": "Integer"}, {"name": "SRC_SYS_CD", "dataType": "String"}, {"name": "CLM_ID", "dataType": "String"}, {"name": "PregnancyDiagDesc", "dataType": "String"}, {"name": "Trimester_INT", "dataType": "Integer"}, {"name": "PregnancyDiag", "dataType": "String"}, {"name": "CLM_SVC_STRT_DT_SK", "dataType": "Timestamp"}, {"name": "MBR_ID", "dataType": "String"}, {"name": "Trimester", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_103
