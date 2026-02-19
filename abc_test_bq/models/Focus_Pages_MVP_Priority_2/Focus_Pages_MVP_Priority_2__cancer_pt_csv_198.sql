{{
  config({    
    "materialized": "table",
    "alias": "cancer_pt_csv_198_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH DiagnosisRollup_184 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'DiagnosisRollup_184_ref') }}

),

AlteryxSelect_188 AS (

  SELECT 
    NULL AS ICD_DIAG_ROLLUP_DESC,
    * EXCEPT (`ICD-10-CM Code Description`, 
    `CCSR Category`, 
    `Inpatient Default CCSR paranthesesOpenYslashNslashXparanthesesClose`, 
    `Outpatient Default CCSR paranthesesOpenYslashNslashXparanthesesClose`)
  
  FROM DiagnosisRollup_184 AS in0

),

Unique_189_before AS (

  SELECT 
    `ICD-10-CM Code` AS `ICD__10__CM Code`,
    * EXCEPT (`ICD-10-CM Code`)
  
  FROM AlteryxSelect_188 AS in0

),

Unique_189 AS (

  SELECT * 
  
  FROM Unique_189_before AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `ICD__10__CM Code`, `CCSR Category Description` ORDER BY `ICD__10__CM Code`, `CCSR Category Description`) = 1

),

Unique_189_after AS (

  SELECT 
    `ICD__10__CM Code` AS `ICD-10-CM Code`,
    * EXCEPT (`ICD__10__CM Code`)
  
  FROM Unique_189 AS in0

),

aka_alxaa2_Quer_1 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_1_ref') }}

),

Summarize_176 AS (

  SELECT 
    MAX(CLM_SVC_STRT_DT_SK) AS Max_CLM_SVC_STRT_DT_SK,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    DIAG_CD_DESC AS DIAG_CD_DESC,
    DIAG_CD AS DIAG_CD
  
  FROM aka_alxaa2_Quer_1 AS in0
  
  GROUP BY 
    MBR_INDV_BE_KEY, DIAG_CD_DESC, DIAG_CD

),

Sort_180 AS (

  SELECT * 
  
  FROM Summarize_176 AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, Max_CLM_SVC_STRT_DT_SK DESC

),

Join_193_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Sort_180 AS in0
  INNER JOIN Unique_189_after AS in1
     ON (in0.DIAG_CD = in1.`ICD-10-CM Code`)

),

Sort_197 AS (

  SELECT * 
  
  FROM Join_193_inner AS in0
  
  ORDER BY `CCSR Category Description` ASC

)

SELECT *

FROM Sort_197
