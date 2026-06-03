{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_220_to_Formula_286_2 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_220_to_Formula_286_2')}}

),

Formula_273_1 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Formula_273_1')}}

),

AlteryxSelect_271 AS (

  SELECT 
    PRD_END_DT AS PRD_END_DT,
    Heritage AS Heritage,
    CAST(CD_BANK AS string) AS CD_BANK,
    HCS_LOB_NODE04_NB AS HCS_LOB_NODE04_NB,
    LOB_DESC AS LOB_DESC,
    CCXREF_SAP_COST_CENTER AS CCXREF_SAP_COST_CENTER,
    CD_VALUE AS CD_VALUE,
    Count AS `Count`,
    HCS_LOB_NODE03_NB AS HCS_LOB_NODE03_NB,
    HCS_LOB_NODE04_Orig AS HCS_LOB_NODE04_Orig,
    RecordID AS RecordID
  
  FROM Formula_273_1 AS in0

),

Summarize_274 AS (

  SELECT 
    SUM(CD_VALUE) AS CD_VALUE,
    SUM(Count) AS CNT,
    LOB_DESC AS LOB_DESC,
    Heritage AS Heritage,
    CD_BANK AS CD_BANK,
    CCXREF_SAP_COST_CENTER AS CCXREF_SAP_COST_CENTER,
    PRD_END_DT AS PRD_END_DT,
    HCS_LOB_NODE03_NB AS HCS_LOB_NODE03_NB
  
  FROM AlteryxSelect_271 AS in0
  
  GROUP BY 
    LOB_DESC, Heritage, CD_BANK, CCXREF_SAP_COST_CENTER, PRD_END_DT, HCS_LOB_NODE03_NB

),

AppendFields_283 AS (

  SELECT 
    in1.LOB_DESC AS LOB_DESC,
    in1.CCXREF_SAP_COST_CENTER AS CCXREF_SAP_COST_CENTER,
    in1.CD_VALUE AS CD_VALUE,
    in1.HCS_LOB_NODE03_NB AS HCS_LOB_NODE03_NB,
    in0.FilePath2 AS FilePath2,
    in1.PRD_END_DT AS PRD_END_DT,
    in1.CD_BANK AS CD_BANK,
    in1.CNT AS CNT,
    in1.Heritage AS Heritage
  
  FROM Formula_220_to_Formula_286_2 AS in0
  INNER JOIN Summarize_274 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_283
