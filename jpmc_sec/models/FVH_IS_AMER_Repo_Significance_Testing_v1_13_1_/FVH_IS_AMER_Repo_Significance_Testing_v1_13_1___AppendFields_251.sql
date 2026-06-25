{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_235_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_235_0')}}

),

Formula_90_1 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_90_1')}}

),

AppendFields_251 AS (

  SELECT 
    in1.Portfolio AS Portfolio,
    in1.Curve AS Curve,
    in1.Ratio AS Ratio,
    in1.TradeID AS TradeID,
    in1.Region AS Region,
    in1.ConcatFind AS ConcatFind,
    in1.`Asset Or Liability` AS `Asset Or Liability`,
    in1.MTM AS MTM,
    in1.`MTM Ratio` AS `MTM Ratio`,
    in0.`MTM in reporting` AS `MTM in reporting`,
    in1.Product AS Product,
    in1.Level AS Level
  
  FROM Formula_235_0 AS in0
  INNER JOIN Formula_90_1 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_251
