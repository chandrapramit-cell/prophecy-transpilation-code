{{
  config({    
    "materialized": "table",
    "alias": "msk_members_csv_72_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_54 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_54_ref') }}

),

Summarize_63 AS (

  SELECT 
    COUNT(DISTINCT CLM_LN_SVC_STRT_DT_SK) AS CountDistinct_CLM_LN_SVC_STRT_DT_SK,
    SUM(CLM_LN_ALW_AMT) AS Sum_CLM_LN_ALW_AMT,
    MIN(CLM_LN_SVC_STRT_DT_SK) AS Min_CLM_LN_SVC_STRT_DT_SK,
    MAX(CLM_LN_SVC_STRT_DT_SK) AS Max_CLM_LN_SVC_STRT_DT_SK,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM aka_alxaa2_Quer_54 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

Formula_69_0 AS (

  SELECT 
    'Back Surgery' AS `MSK Category`,
    *
  
  FROM Summarize_63 AS in0

),

aka_alxaa2_Quer_57 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_57_ref') }}

),

Summarize_60 AS (

  SELECT 
    COUNT(DISTINCT CLM_LN_SVC_STRT_DT_SK) AS CountDistinct_CLM_LN_SVC_STRT_DT_SK,
    SUM(CLM_LN_ALW_AMT) AS Sum_CLM_LN_ALW_AMT,
    MIN(CLM_LN_SVC_STRT_DT_SK) AS Min_CLM_LN_SVC_STRT_DT_SK,
    MAX(CLM_LN_SVC_STRT_DT_SK) AS Max_CLM_LN_SVC_STRT_DT_SK,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM aka_alxaa2_Quer_57 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

Formula_66_0 AS (

  SELECT 
    'Knee Replacement' AS `MSK Category`,
    *
  
  FROM Summarize_60 AS in0

),

aka_alxaa2_Quer_55 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_55_ref') }}

),

Summarize_62 AS (

  SELECT 
    COUNT(DISTINCT CLM_LN_SVC_STRT_DT_SK) AS CountDistinct_CLM_LN_SVC_STRT_DT_SK,
    SUM(CLM_LN_ALW_AMT) AS Sum_CLM_LN_ALW_AMT,
    MIN(CLM_LN_SVC_STRT_DT_SK) AS Min_CLM_LN_SVC_STRT_DT_SK,
    MAX(CLM_LN_SVC_STRT_DT_SK) AS Max_CLM_LN_SVC_STRT_DT_SK,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM aka_alxaa2_Quer_55 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

Formula_68_0 AS (

  SELECT 
    'Neck Surgery' AS `MSK Category`,
    *
  
  FROM Summarize_62 AS in0

),

aka_alxaa2_Quer_58 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_58_ref') }}

),

Summarize_59 AS (

  SELECT 
    COUNT(DISTINCT CLM_LN_SVC_STRT_DT_SK) AS CountDistinct_CLM_LN_SVC_STRT_DT_SK,
    SUM(CLM_LN_ALW_AMT) AS Sum_CLM_LN_ALW_AMT,
    MIN(CLM_LN_SVC_STRT_DT_SK) AS Min_CLM_LN_SVC_STRT_DT_SK,
    MAX(CLM_LN_SVC_STRT_DT_SK) AS Max_CLM_LN_SVC_STRT_DT_SK,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM aka_alxaa2_Quer_58 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

Formula_65_0 AS (

  SELECT 
    'Hip Procedure' AS `MSK Category`,
    *
  
  FROM Summarize_59 AS in0

),

aka_alxaa2_Quer_56 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_56_ref') }}

),

Summarize_61 AS (

  SELECT 
    COUNT(DISTINCT CLM_LN_SVC_STRT_DT_SK) AS CountDistinct_CLM_LN_SVC_STRT_DT_SK,
    SUM(CLM_LN_ALW_AMT) AS Sum_CLM_LN_ALW_AMT,
    MIN(CLM_LN_SVC_STRT_DT_SK) AS Min_CLM_LN_SVC_STRT_DT_SK,
    MAX(CLM_LN_SVC_STRT_DT_SK) AS Max_CLM_LN_SVC_STRT_DT_SK,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM aka_alxaa2_Quer_56 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

Formula_67_0 AS (

  SELECT 
    'Shoulder Surgery' AS `MSK Category`,
    *
  
  FROM Summarize_61 AS in0

),

aka_alxaa2_Quer_53 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_53_ref') }}

),

Summarize_64 AS (

  SELECT 
    COUNT(DISTINCT CLM_LN_SVC_STRT_DT_SK) AS CountDistinct_CLM_LN_SVC_STRT_DT_SK,
    SUM(CLM_LN_ALW_AMT) AS Sum_CLM_LN_ALW_AMT,
    MIN(CLM_LN_SVC_STRT_DT_SK) AS Min_CLM_LN_SVC_STRT_DT_SK,
    MAX(CLM_LN_SVC_STRT_DT_SK) AS Max_CLM_LN_SVC_STRT_DT_SK,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM aka_alxaa2_Quer_53 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

Formula_163_0 AS (

  SELECT 
    'Other Potential MSK Related Diagnosis' AS `MSK Category`,
    *
  
  FROM Summarize_64 AS in0

),

Union_71 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_66_0', 'Formula_68_0', 'Formula_65_0', 'Formula_67_0', 'Formula_163_0', 'Formula_69_0'], 
      [
        '[{"name": "Sum_CLM_LN_ALW_AMT", "dataType": "Decimal"}, {"name": "MBR_INDV_BE_KEY", "dataType": "Decimal"}, {"name": "MSK Category", "dataType": "String"}, {"name": "CountDistinct_CLM_LN_SVC_STRT_DT_SK", "dataType": "Double"}, {"name": "Min_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}, {"name": "Max_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}]', 
        '[{"name": "Sum_CLM_LN_ALW_AMT", "dataType": "Decimal"}, {"name": "MBR_INDV_BE_KEY", "dataType": "Decimal"}, {"name": "MSK Category", "dataType": "String"}, {"name": "CountDistinct_CLM_LN_SVC_STRT_DT_SK", "dataType": "Double"}, {"name": "Min_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}, {"name": "Max_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}]', 
        '[{"name": "Sum_CLM_LN_ALW_AMT", "dataType": "Decimal"}, {"name": "MBR_INDV_BE_KEY", "dataType": "Decimal"}, {"name": "MSK Category", "dataType": "String"}, {"name": "CountDistinct_CLM_LN_SVC_STRT_DT_SK", "dataType": "Double"}, {"name": "Min_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}, {"name": "Max_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}]', 
        '[{"name": "Sum_CLM_LN_ALW_AMT", "dataType": "Decimal"}, {"name": "MBR_INDV_BE_KEY", "dataType": "Decimal"}, {"name": "MSK Category", "dataType": "String"}, {"name": "CountDistinct_CLM_LN_SVC_STRT_DT_SK", "dataType": "Double"}, {"name": "Min_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}, {"name": "Max_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}]', 
        '[{"name": "Sum_CLM_LN_ALW_AMT", "dataType": "Decimal"}, {"name": "MBR_INDV_BE_KEY", "dataType": "Decimal"}, {"name": "MSK Category", "dataType": "String"}, {"name": "CountDistinct_CLM_LN_SVC_STRT_DT_SK", "dataType": "Double"}, {"name": "Min_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}, {"name": "Max_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}]', 
        '[{"name": "Sum_CLM_LN_ALW_AMT", "dataType": "Decimal"}, {"name": "MBR_INDV_BE_KEY", "dataType": "Decimal"}, {"name": "MSK Category", "dataType": "String"}, {"name": "CountDistinct_CLM_LN_SVC_STRT_DT_SK", "dataType": "Double"}, {"name": "Min_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}, {"name": "Max_CLM_LN_SVC_STRT_DT_SK", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_166_0 AS (

  SELECT 
    CAST(CAST((
      DATE_DIFF(
        (PARSE_DATE('%Y-%m-%d', CAST(CURRENT_DATE AS STRING))), 
        (PARSE_DATE('%Y-%m-%d', MAX_CLM_LN_SVC_STRT_DT_SK)), 
        MONTH)
      / 12
    ) AS INTEGER) AS STRING) AS `Time Since Most Recent Diagnosis`,
    CAST(CAST((
      DATE_DIFF(
        (PARSE_DATE('%Y-%m-%d', CAST(CURRENT_DATE AS STRING))), 
        (PARSE_DATE('%Y-%m-%d', MIN_CLM_LN_SVC_STRT_DT_SK)), 
        MONTH)
      / 12
    ) AS INTEGER) AS STRING) AS `Time Since First Diag`,
    *
  
  FROM Union_71 AS in0

),

Formula_166_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`Time Since Most Recent Diagnosis` > '4')
          THEN '5 or more years'
        ELSE CAST((CONCAT(`Time Since Most Recent Diagnosis`, ' year(s)')) AS STRING)
      END
    ) AS STRING) AS `Time Since Most Recent Diagnosis`,
    CAST((
      CASE
        WHEN (`Time Since First Diag` > '4')
          THEN '5 or more years'
        ELSE CAST((CONCAT(`Time Since First Diag`, ' year(s)')) AS STRING)
      END
    ) AS STRING) AS `Time Since First Diag`,
    * EXCEPT (`time since first diag`, `time since most recent diagnosis`)
  
  FROM Formula_166_0 AS in0

)

SELECT *

FROM Formula_166_1
