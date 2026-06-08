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

Join_142_inner AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Join_142_inner')}}

),

AppendFields_253 AS (

  SELECT 
    in1.`Avg_Effective Mult` AS `Avg_Effective Mult`,
    in1.`Sum_Abs Risk` AS `Sum_Abs Risk`,
    in1.`Uncertainty Tweak` AS `Uncertainty Tweak`,
    in1.Curve AS Curve,
    in1.Sum_MTM AS Sum_MTM,
    in1.StdevCalc AS StdevCalc,
    in1.Avg_SignificanceTest AS Avg_SignificanceTest,
    in0.Reporting AS Reporting,
    in1.`Sum_Stdev Weighed` AS `Sum_Stdev Weighed`,
    in1.TradeID AS TradeID,
    in1.`Sum_Amount Total` AS `Sum_Amount Total`,
    in1.Region AS Region,
    in1.Sum_Impact AS Sum_Impact,
    in1.`Asset Or Liability` AS `Asset Or Liability`,
    in1.`Impact in MM` AS `Impact in MM`,
    in1.Avg_Std AS Avg_Std,
    in1.Product AS Product,
    in1.Level AS Level
  
  FROM Formula_235_0 AS in0
  INNER JOIN Join_142_inner AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_253
