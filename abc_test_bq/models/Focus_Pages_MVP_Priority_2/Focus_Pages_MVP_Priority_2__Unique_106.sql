{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_157_to_Filter_97 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Filter_157_to_Filter_97')}}

),

Filter_114 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Filter_114')}}

),

Union_115 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_157_to_Filter_97', 'Filter_114'], 
      [
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "SUB_ID", "dataType": "String"}, {"name": "ServiceDate", "dataType": "Date"}, {"name": "MBR_SK", "dataType": "Integer"}, {"name": "SRC_SYS_CD", "dataType": "String"}, {"name": "CLM_ID", "dataType": "String"}, {"name": "PregnancyDiagDesc", "dataType": "String"}, {"name": "PregnancyDiag", "dataType": "String"}, {"name": "CLM_SVC_STRT_DT_SK", "dataType": "Timestamp"}, {"name": "MBR_ID", "dataType": "String"}]', 
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "SUB_ID", "dataType": "String"}, {"name": "ServiceDate", "dataType": "Date"}, {"name": "MBR_SK", "dataType": "Integer"}, {"name": "SRC_SYS_CD", "dataType": "String"}, {"name": "CLM_ID", "dataType": "String"}, {"name": "PregnancyDiagDesc", "dataType": "String"}, {"name": "PregnancyDiag", "dataType": "String"}, {"name": "CLM_SVC_STRT_DT_SK", "dataType": "Timestamp"}, {"name": "MBR_ID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_105 AS (

  SELECT MBR_ID AS MBR_ID
  
  FROM Union_115 AS in0

),

Unique_106 AS (

  SELECT * 
  
  FROM AlteryxSelect_105 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MBR_ID ORDER BY MBR_ID) = 1

)

SELECT *

FROM Unique_106
