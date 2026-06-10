{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Join_373_inner AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Join_373_inner')}}

),

Database__LOADI_283 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_283') }}

),

Join_384_inner AS (

  SELECT 
    in0.* EXCEPT (`OD`, `FLT_NBR`, `DPTR_DATE`, `CAP`),
    in1.* EXCEPT (`LEG_NDOD`, `TT_NDOD`, `DPTR_WEEK`, `SUM(REV.LEG_PAX_CNT)`)
  
  FROM Join_373_inner AS in0
  INNER JOIN Database__LOADI_283 AS in1
     ON (in0.NDOD = in1.LEG_NDOD)

)

SELECT *

FROM Join_384_inner
