{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH DynamicInput_211 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing', 'DynamicInput_211') }}

),

AlteryxSelect_42 AS (

  SELECT 
    `2026-02-05` AS F1,
    * EXCEPT (`F2`, `2026-02-05`)
  
  FROM DynamicInput_211 AS in0

)

SELECT *

FROM AlteryxSelect_42
