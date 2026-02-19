{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_135_inner_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Join_135_inner_UnionLeftOuter')}}

),

AlteryxSelect_130 AS (

  SELECT 
    MBR_ID AS MBR_ID,
    MBR_SK AS MBR_SK,
    `Service Date` AS `Service Date`,
    PregnancyDiag AS PregnancyDiag,
    PregnancyDiagDesc AS PregnancyDiagDesc,
    `Claim ID` AS `Claim ID`,
    Trimester AS Trimester,
    Trimester_INT AS Trimester_INT,
    `Identified High Risk Pregnancy` AS `Identified High Risk Pregnancy`,
    SUB_ID AS SUB_ID,
    MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    * EXCEPT (`SRC_SYS_CD`, 
    `MBR_ID`, 
    `MBR_SK`, 
    `Service Date`, 
    `PregnancyDiag`, 
    `PregnancyDiagDesc`, 
    `Claim ID`, 
    `Trimester`, 
    `Trimester_INT`, 
    `Identified High Risk Pregnancy`, 
    `SUB_ID`, 
    `MBR_INDV_BE_KEY`)
  
  FROM Join_135_inner_UnionLeftOuter AS in0

),

Unique_110 AS (

  SELECT * 
  
  FROM AlteryxSelect_130 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MBR_ID, 
  MBR_SK, 
  `Service Date`, 
  PregnancyDiag, 
  PregnancyDiagDesc, 
  `Claim ID`, 
  Trimester, 
  Trimester_INT, 
  `Identified High Risk Pregnancy`, 
  SUB_ID, 
  MBR_INDV_BE_KEY ORDER BY MBR_ID, MBR_SK, `Service Date`, PregnancyDiag, PregnancyDiagDesc, `Claim ID`, Trimester, Trimester_INT, `Identified High Risk Pregnancy`, SUB_ID, MBR_INDV_BE_KEY) = 1

)

SELECT *

FROM Unique_110
