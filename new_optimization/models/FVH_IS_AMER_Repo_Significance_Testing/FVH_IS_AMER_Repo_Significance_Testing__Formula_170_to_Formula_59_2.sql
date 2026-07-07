{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_168_inner AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Join_168_inner')}}

),

Formula_170_to_Formula_59_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((CP_TYPE = 'SUB IG HIGH') AND (`VA Map` IN ('30Y', '20Y', '10Y', '7Y', '2Y', '5Y')))
          THEN 25
        ELSE Std
      END
    ) AS DOUBLE) AS Std,
    * EXCEPT (`std`)
  
  FROM Join_168_inner AS in0

),

Formula_170_to_Formula_59_1 AS (

  SELECT 
    CAST((-1 * ABS(((2 * `Amount Total`) * Std))) AS DOUBLE) AS Impact,
    CAST((Std * 2) AS DOUBLE) AS `Effective Mult`,
    CAST(ABS(`Amount Total`) AS DOUBLE) AS `Abs Risk`,
    *
  
  FROM Formula_170_to_Formula_59_0 AS in0

),

Formula_170_to_Formula_59_2 AS (

  SELECT 
    CAST((`Abs Risk` * Std) AS DOUBLE) AS `Stdev Weighed`,
    *
  
  FROM Formula_170_to_Formula_59_1 AS in0

)

SELECT *

FROM Formula_170_to_Formula_59_2
