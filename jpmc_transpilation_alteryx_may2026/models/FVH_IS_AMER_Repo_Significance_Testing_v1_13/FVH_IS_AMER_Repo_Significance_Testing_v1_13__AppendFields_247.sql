{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_235_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Formula_235_0')}}

),

Summarize_60 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Summarize_60')}}

),

Unique_65 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Unique_65')}}

),

Join_30_left AS (

  SELECT in0.*
  
  FROM Summarize_60 AS in0
  ANTI JOIN Unique_65 AS in1
     ON (in0.TradeID = in1.TradeID)

),

AppendFields_247 AS (

  SELECT 
    in1.Portfolio AS Portfolio,
    in1.TradeID AS TradeID,
    in1.Region AS Region,
    in1.Sum_Impact AS Sum_Impact,
    in0.`Missing MTM` AS `Missing MTM`
  
  FROM Formula_235_0 AS in0
  INNER JOIN Join_30_left AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_247
