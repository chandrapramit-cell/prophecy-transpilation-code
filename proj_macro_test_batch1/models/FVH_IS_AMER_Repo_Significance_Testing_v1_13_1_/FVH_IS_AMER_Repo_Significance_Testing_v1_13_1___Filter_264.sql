{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_168_inner AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_168_inner')}}

),

Filter_264 AS (

  SELECT * 
  
  FROM Join_168_inner AS in0
  
  WHERE (TradeID = 'CBN_Upsize_5B_83013581')

)

SELECT *

FROM Filter_264
