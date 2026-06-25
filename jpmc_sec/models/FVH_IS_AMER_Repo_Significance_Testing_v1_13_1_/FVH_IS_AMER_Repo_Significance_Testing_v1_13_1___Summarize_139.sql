{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_72_inner_formula_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_72_inner_formula_0')}}

),

Filter_138 AS (

  SELECT * 
  
  FROM Join_72_inner_formula_0 AS in0
  
  WHERE (CAST(Count AS INTEGER) >= 2)

),

Summarize_139 AS (

  SELECT 
    DISTINCT `Source System` AS `Source System`,
    TradeID AS TradeID,
    Curve AS Curve
  
  FROM Filter_138 AS in0

)

SELECT *

FROM Summarize_139
