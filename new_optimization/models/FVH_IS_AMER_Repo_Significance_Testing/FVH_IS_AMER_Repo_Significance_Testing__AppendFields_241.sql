{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_235_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Formula_235_0')}}

),

Summarize_139 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Summarize_139')}}

),

DynamicInput_215 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing', 'DynamicInput_215') }}

),

Join_140_left AS (

  SELECT in0.*
  
  FROM Summarize_139 AS in0
  ANTI JOIN DynamicInput_215 AS in1
     ON ((in0.TradeID = in1.`Trade id`) AND (in0.Curve = in1.Curve))

),

AppendFields_241 AS (

  SELECT 
    in1.`Source System` AS `Source System`,
    in1.TradeID AS TradeID,
    in1.Curve AS Curve,
    in0.`Notional Ratio Missing` AS `Notional Ratio Missing`
  
  FROM Formula_235_0 AS in0
  INNER JOIN Join_140_left AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_241
