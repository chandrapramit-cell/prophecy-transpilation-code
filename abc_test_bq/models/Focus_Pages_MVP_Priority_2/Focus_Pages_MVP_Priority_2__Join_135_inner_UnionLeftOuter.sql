{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH aka_alxaa2_Quer_134 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_134_ref') }}

),

Join_109_inner_formula_0 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Join_109_inner_formula_0')}}

),

Formula_136_0 AS (

  SELECT 
    'Yes' AS `Identified High Risk Pregnancy`,
    *
  
  FROM aka_alxaa2_Quer_134 AS in0

),

Unique_140 AS (

  SELECT * 
  
  FROM Formula_136_0 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MBR_ID ORDER BY MBR_ID) = 1

),

Join_135_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.MBR_ID = in1.MBR_ID)
          THEN NULL
        ELSE 'No'
      END
    ) AS `Identified High Risk Pregnancy`,
    in0.* EXCEPT (`SUB_ID`, `MBR_SK`, `SRC_SYS_CD`, `PREGNANCYDIAGDESC`, `CLM_ID`, `PREGNANCYDIAG`, `CLM_SVC_STRT_DT_SK`, `MBR_ID`),
    in1.* EXCEPT (`Identified High Risk Pregnancy`)
  
  FROM Join_109_inner_formula_0 AS in0
  LEFT JOIN Unique_140 AS in1
     ON (in0.MBR_ID = in1.MBR_ID)

)

SELECT *

FROM Join_135_inner_UnionLeftOuter
