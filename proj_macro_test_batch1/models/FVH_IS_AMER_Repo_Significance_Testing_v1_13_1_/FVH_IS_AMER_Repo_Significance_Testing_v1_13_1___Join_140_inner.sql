{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_139 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Summarize_139')}}

),

DynamicInput_215 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1_', 'DynamicInput_215') }}

),

Join_140_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Concat`, `Trade id`, `Curve`, `Ratio`, `Trade`)
  
  FROM Summarize_139 AS in0
  INNER JOIN DynamicInput_215 AS in1
     ON ((in0.TradeID = in1.`Trade id`) AND (in0.Curve = in1.Curve))

)

SELECT *

FROM Join_140_inner
