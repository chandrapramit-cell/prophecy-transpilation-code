{{
  config({    
    "materialized": "table",
    "alias": "pregnancy_compl_170_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_167 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_167_ref') }}

),

Summarize_168 AS (

  SELECT 
    MIN(CLM_SVC_STRT_DT_SK) AS Min_CLM_SVC_STRT_DT_SK,
    MAX(CLM_SVC_STRT_DT_SK) AS Max_CLM_SVC_STRT_DT_SK,
    SUM(CLM_LN_TOT_ALW_AMT) AS Sum_CLM_LN_TOT_ALW_AMT,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY
  
  FROM aka_alxaa2_Quer_167 AS in0
  
  GROUP BY MBR_INDV_BE_KEY

),

Formula_169_0 AS (

  SELECT 
    1 AS `Fertility Complications`,
    *
  
  FROM Summarize_168 AS in0

)

SELECT *

FROM Formula_169_0
