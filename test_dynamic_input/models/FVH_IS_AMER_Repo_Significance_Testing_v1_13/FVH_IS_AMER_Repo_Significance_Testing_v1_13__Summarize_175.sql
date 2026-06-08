{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_36_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Union_36_postRename')}}

),

Summarize_175 AS (

  SELECT 
    DISTINCT TradeID AS TradeID,
    Curve AS Curve,
    Portfolio AS Portfolio,
    Tenor AS Tenor
  
  FROM Union_36_postRename AS in0

)

SELECT *

FROM Summarize_175
