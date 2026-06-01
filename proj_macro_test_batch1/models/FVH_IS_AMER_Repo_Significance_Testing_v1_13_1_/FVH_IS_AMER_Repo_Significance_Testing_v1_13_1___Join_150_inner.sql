{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_190_left AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_190_left')}}

),

Union_197_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_197_postRename')}}

),

Join_150_inner AS (

  SELECT 
    in1.Region AS Right_Region,
    in1.TradeID AS Right_TradeID,
    in1.Portfolio AS Right_Portfolio,
    in0.*,
    in1.* EXCEPT (`Region`, `TradeID`, `Portfolio`)
  
  FROM Join_190_left AS in0
  INNER JOIN Union_197_postRename AS in1
     ON (in0.TradeID = in1.TradeID)

)

SELECT *

FROM Join_150_inner
