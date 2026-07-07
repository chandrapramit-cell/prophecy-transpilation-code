{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_190_left AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Join_190_left')}}

),

AlteryxSelect_68 AS (

  SELECT 
    `Source System` AS `Source System`,
    Curve AS Curve,
    TradeID AS TradeID
  
  FROM Join_190_left AS in0

),

Summarize_69 AS (

  SELECT 
    DISTINCT `Source System` AS `Source System`,
    Curve AS Curve,
    TradeID AS TradeID
  
  FROM AlteryxSelect_68 AS in0

),

Summarize_71 AS (

  SELECT 
    *,
    COUNT(
      (
        CASE
          WHEN ((Curve IS NULL) OR (CAST(Curve AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) OVER (PARTITION BY `Source System`, TradeID ORDER BY 1 ASC NULLS FIRST) AS `Count`
  
  FROM Summarize_69 AS in0

),

Join_72_inner_formula_0 AS (

  SELECT 
    `Source System` AS `Source System`,
    Curve AS Curve,
    TradeID AS TradeID,
    Count AS `Count`,
    * EXCEPT (`source system`, `curve`, `tradeid`, `count`)
  
  FROM Summarize_71 AS in0

)

SELECT *

FROM Join_72_inner_formula_0
