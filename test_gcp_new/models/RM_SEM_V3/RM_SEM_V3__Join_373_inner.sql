{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Summarize_370 AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Summarize_370')}}

),

Filter_372 AS (

  SELECT * 
  
  FROM Summarize_370 AS in0
  
  WHERE (Count >= 10)

),

Database__LOADI_280 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_280') }}

),

Join_373_inner AS (

  SELECT 
    in0.* EXCEPT (`NDOD`, `Count`),
    in1.*
  
  FROM Filter_372 AS in0
  INNER JOIN Database__LOADI_280 AS in1
     ON (in0.NDOD = in1.NDOD)

)

SELECT *

FROM Join_373_inner
