{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_36_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testingneww__Union_36_postRename')}}

),

Union_191_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testingneww__Union_191_postRename')}}

),

Join_190_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`TradeID`, 
    `Curve`, 
    `Portfolio`, 
    `Totem Curve`, 
    `Max_Tenor Map`, 
    `Right_Totem Curve`, 
    `PortfolioType`, 
    `Backtested Trades`)
  
  FROM Union_36_postRename AS in0
  INNER JOIN Union_191_postRename AS in1
     ON (in0.TradeID = in1.TradeID)

)

SELECT *

FROM Join_190_inner
