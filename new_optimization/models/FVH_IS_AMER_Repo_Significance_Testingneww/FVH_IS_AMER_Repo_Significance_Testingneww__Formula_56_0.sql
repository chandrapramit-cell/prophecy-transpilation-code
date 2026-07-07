{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH DynamicInput_213 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testingneww', 'DynamicInput_213') }}

),

Filter_55 AS (

  SELECT * 
  
  FROM DynamicInput_213 AS in0
  
  WHERE ((CP_TYPE = 'IG') AND (COUNTERPARTY = 'ALL'))

),

Formula_56_0 AS (

  SELECT 
    CAST((CONCAT(VA_CURVE, TENOR)) AS string) AS Concat,
    *
  
  FROM Filter_55 AS in0

)

SELECT *

FROM Formula_56_0
