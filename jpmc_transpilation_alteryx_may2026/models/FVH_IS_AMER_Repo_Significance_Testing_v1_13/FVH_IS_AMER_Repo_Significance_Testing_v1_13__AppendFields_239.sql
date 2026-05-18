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

Union_191_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Union_191_postRename')}}

),

AppendFields_239 AS (

  SELECT 
    in0.`Backtested Trades` AS `Source_Backtested Trades`,
    in1.PortfolioType AS PortfolioType,
    in1.Portfolio AS Portfolio,
    in1.Curve AS Curve,
    in1.`Right_Totem Curve` AS `Right_Totem Curve`,
    in1.`Totem Curve` AS `Totem Curve`,
    in1.`Max_Tenor Map` AS `Max_Tenor Map`,
    in1.`Backtested Trades` AS `Backtested Trades`,
    in1.TradeID AS TradeID
  
  FROM Formula_235_0 AS in0
  INNER JOIN Union_191_postRename AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_239
