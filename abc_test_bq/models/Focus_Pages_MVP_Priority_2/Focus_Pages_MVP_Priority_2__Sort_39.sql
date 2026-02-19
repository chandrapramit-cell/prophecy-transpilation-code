{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH aka_alxaa2_Quer_29 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_29_ref') }}

),

Formula_35_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((STRPOS((coalesce(LOWER(DIAG_CD_DESC), '')), LOWER('Type 1'))) > 0)
          THEN '1. Type 1'
        WHEN ((STRPOS((coalesce(LOWER(DIAG_CD_DESC), '')), LOWER('Type 2'))) > 0)
          THEN '2. Type 2'
        ELSE '3. Unsure'
      END
    ) AS STRING) AS `Diabetes Type`,
    *
  
  FROM aka_alxaa2_Quer_29 AS in0

),

Summarize_37 AS (

  SELECT 
    MIN(CLM_SVC_STRT_DT_SK) AS Min_CLM_SVC_STRT_DT_SK,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    `Diabetes Type` AS `Diabetes Type`
  
  FROM Formula_35_0 AS in0
  
  GROUP BY 
    MBR_INDV_BE_KEY, `Diabetes Type`

),

Sort_88 AS (

  SELECT * 
  
  FROM Summarize_37 AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, `Diabetes Type` ASC

),

Filter_41_reject AS (

  SELECT * 
  
  FROM Sort_88 AS in0
  
  WHERE (
          (
            NOT(
              `Diabetes Type` <> 'Type 3 (Unsure)')
          )
          OR ((`Diabetes Type` <> 'Type 3 (Unsure)') IS NULL)
        )

),

Filter_41 AS (

  SELECT * 
  
  FROM Sort_88 AS in0
  
  WHERE (`Diabetes Type` <> 'Type 3 (Unsure)')

),

Join_42_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.MBR_INDV_BE_KEY
      END
    ) AS MBR_INDV_BE_KEY,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.`Diabetes Type`
      END
    ) AS `Diabetes Type`,
    (
      CASE
        WHEN (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)
          THEN NULL
        ELSE in1.Min_CLM_SVC_STRT_DT_SK
      END
    ) AS Min_CLM_SVC_STRT_DT_SK
  
  FROM Filter_41 AS in0
  FULL JOIN Filter_41_reject AS in1
     ON (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)

),

Summarize_44 AS (

  SELECT 
    DISTINCT MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    `Diabetes Type` AS `Diabetes Type`,
    Min_CLM_SVC_STRT_DT_SK AS Min_CLM_SVC_STRT_DT_SK
  
  FROM Join_42_left_UnionFullOuter AS in0

),

Sort_39 AS (

  SELECT * 
  
  FROM Summarize_44 AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, `Diabetes Type` ASC

)

SELECT *

FROM Sort_39
