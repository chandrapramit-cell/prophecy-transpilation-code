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

Union_197_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_197_postRename')}}

),

Join_190_left AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_190_left')}}

),

Join_150_left AS (

  SELECT in0.*
  
  FROM Join_190_left AS in0
  ANTI JOIN Union_197_postRename AS in1
     ON (in0.TradeID = in1.TradeID)

),

AppendFields_256 AS (

  SELECT 
    in1.`Amount Total` AS `Amount Total`,
    in1.CURVE_TYPE AS CURVE_TYPE,
    in1.COLLATERAL_CONTRACT_ID AS COLLATERAL_CONTRACT_ID,
    in1.Portfolio AS Portfolio,
    in1.Curve AS Curve,
    in1.COLLATERAL_TYPE AS COLLATERAL_TYPE,
    in1.ISIN AS ISIN,
    in1.SPN AS SPN,
    in1.Tenor AS Tenor,
    in1.IS_INTERNAL AS IS_INTERNAL,
    in0.`Significance Test Missed Trades` AS `Significance Test Missed Trades`,
    in1.TradeID AS TradeID,
    in1.`Curve Family` AS `Curve Family`,
    in1.Region AS Region,
    in1.`Sub Type` AS `Sub Type`,
    in1.`Risk Type` AS `Risk Type`,
    in1.`Source System` AS `Source System`,
    in1.CURVE_DESCRIPTOR AS CURVE_DESCRIPTOR
  
  FROM Formula_235_0 AS in0
  INNER JOIN Join_150_left AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_256
