{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_197_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Union_197_postRename')}}

),

Formula_170_to_Formula_59_2 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Formula_170_to_Formula_59_2')}}

),

Join_74_inner AS (

  SELECT 
    in1.Region AS Right_Region,
    in0.*,
    in1.* EXCEPT (`Region`, `TradeID`, `Sum_Impact`, `Portfolio`)
  
  FROM Formula_170_to_Formula_59_2 AS in0
  INNER JOIN Union_197_postRename AS in1
     ON (in0.TradeID = in1.TradeID)

),

Formula_118_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((coalesce((CONTAINS(LOWER(Portfolio), LOWER('FPA'))), FALSE)) AS BOOLEAN)
          THEN 'FPA'
        ELSE 'Repo'
      END
    ) AS string) AS Product,
    CAST((
      CASE
        WHEN (MTM >= 0)
          THEN 'Asset'
        ELSE 'Liability'
      END
    ) AS string) AS `Asset or Liability`,
    *
  
  FROM Join_74_inner AS in0

),

Summarize_75 AS (

  SELECT 
    SUM(`Amount Total`) AS `Sum_Amount Total`,
    SUM(`Abs Risk`) AS `Sum_Abs Risk`,
    AVG(Std) AS Avg_Std,
    AVG(`Effective Mult`) AS `Avg_Effective Mult`,
    SUM(Impact) AS Sum_Impact,
    AVG(SignificanceTest) AS Avg_SignificanceTest,
    SUM(`Stdev Weighed`) AS `Sum_Stdev Weighed`,
    Level AS Level,
    `Asset or Liability` AS `Asset or Liability`,
    Region AS Region,
    Curve AS Curve,
    Product AS Product,
    TradeID AS TradeID
  
  FROM Formula_118_0 AS in0
  
  GROUP BY 
    Level, `Asset or Liability`, Region, Curve, Product, TradeID

),

Formula_78_0 AS (

  SELECT 
    CAST((`Sum_Stdev Weighed` / `Sum_Abs Risk`) AS DOUBLE) AS StdevCalc,
    CAST(((-1 * Sum_Impact) / `Sum_Abs Risk`) AS DOUBLE) AS `Uncertainty Tweak`,
    CAST((Sum_Impact / 1000000) AS DOUBLE) AS `Impact in MM`,
    *
  
  FROM Summarize_75 AS in0

),

Formula_90_1 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Formula_90_1')}}

),

Summarize_94 AS (

  SELECT 
    SUM(MTM) AS Sum_MTM,
    Level AS Level,
    Region AS Region,
    Curve AS Curve,
    `Asset Or Liability` AS `Asset Or Liability`,
    Product AS Product
  
  FROM Formula_90_1 AS in0
  
  GROUP BY 
    Level, Region, Curve, `Asset Or Liability`, Product

),

Join_142_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Asset or Liability`, `Region`, `Product`, `Level`, `Curve`)
  
  FROM Summarize_94 AS in0
  INNER JOIN Formula_78_0 AS in1
     ON (
      (
        (
          ((in0.`Asset Or Liability` = in1.`Asset or Liability`) AND (in0.Region = in1.Region))
          AND (in0.Product = in1.Product)
        )
        AND (in0.Level = in1.Level)
      )
      AND (in0.Curve = in1.Curve)
    )

)

SELECT *

FROM Join_142_inner
