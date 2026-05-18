{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_170_to_Formula_59_2 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Formula_170_to_Formula_59_2')}}

),

Summarize_60 AS (

  SELECT 
    SUM(Impact) AS Sum_Impact,
    Region AS Region,
    TradeID AS TradeID,
    Portfolio AS Portfolio
  
  FROM Formula_170_to_Formula_59_2 AS in0
  
  GROUP BY 
    Region, TradeID, Portfolio

)

SELECT *

FROM Summarize_60
