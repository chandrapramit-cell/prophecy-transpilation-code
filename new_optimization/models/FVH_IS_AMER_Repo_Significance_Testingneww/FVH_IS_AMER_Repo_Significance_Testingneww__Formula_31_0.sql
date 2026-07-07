{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH DynamicInput_207 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testingneww', 'DynamicInput_207') }}

),

Formula_8_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testingneww__Formula_8_0')}}

),

Join_17_right AS (

  SELECT in0.*
  
  FROM Formula_8_0 AS in0
  ANTI JOIN DynamicInput_207 AS in1
     ON (in1.Curve = in0.Curve)

),

Formula_31_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`Source System` = 'Kapital')
          THEN SOURCE_TRADE
        ELSE SOURCE_TRADE_ID
      END
    ) AS string) AS TradeID,
    *
  
  FROM Join_17_right AS in0

)

SELECT *

FROM Formula_31_0
