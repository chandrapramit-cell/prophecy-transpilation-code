{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Production_xlsx_1668 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Production_xlsx_1668_ref') }}

),

AlteryxSelect_774 AS (

  SELECT 
    MBR_INDV_BE_KEY AS `Member Individual Business Entity Key`,
    DSCHG_DT AS READMIN_DSCHG_DT,
    `Prediction Value` AS `Prediction Value`,
    `Prediction Score` AS `Prediction Score`,
    DIAG_CD_DESC AS ADMITTING_DX,
    UNIQUE_DRUG_CT_CURRENT AS UNIQUE_DRUG_CT_CURRENT
  
  FROM Production_xlsx_1668 AS in0

),

Formula_775_0 AS (

  SELECT 
    1 AS READMITPREDICTION,
    *
  
  FROM AlteryxSelect_774 AS in0

),

Union_866 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_866')}}

),

Join_776_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)
          THEN NULL
        ELSE in1.`Member Individual Business Entity Key`
      END
    ) AS `Member Individual Business Entity Key`,
    in0.* EXCEPT (`Member Individual Business Entity Key`),
    in1.* EXCEPT (`Member Individual Business Entity Key`)
  
  FROM Union_866 AS in0
  FULL JOIN Formula_775_0 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)

)

SELECT *

FROM Join_776_left_UnionFullOuter
