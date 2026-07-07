{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_168_inner AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testingneww__Join_168_inner')}}

),

Filter_264_reject AS (

  SELECT * 
  
  FROM Join_168_inner AS in0
  
  WHERE (
          (
            NOT(
              TradeID = 'CBN_Upsize_5B_83013581')
          ) OR ((TradeID = 'CBN_Upsize_5B_83013581') IS NULL)
        )

)

SELECT *

FROM Filter_264_reject
