{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_235_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testingneww__Formula_235_0')}}

),

Join_72_inner_formula_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testingneww__Join_72_inner_formula_0')}}

),

AppendFields_243 AS (

  SELECT 
    in1.Count AS `Count`,
    in1.Curve AS Curve,
    in0.TradeCurveList AS TradeCurveList,
    in1.TradeID AS TradeID,
    in1.`Source System` AS `Source System`
  
  FROM Formula_235_0 AS in0
  INNER JOIN Join_72_inner_formula_0 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_243
