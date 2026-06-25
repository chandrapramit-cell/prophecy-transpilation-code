{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_226 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1__AlteryxSelect_226')}}

),

Summarize_124 AS (

  SELECT 
    DISTINCT variableDate AS variableDate,
    Currency AS Currency,
    FX AS FX
  
  FROM AlteryxSelect_226 AS in0

)

SELECT *

FROM Summarize_124
