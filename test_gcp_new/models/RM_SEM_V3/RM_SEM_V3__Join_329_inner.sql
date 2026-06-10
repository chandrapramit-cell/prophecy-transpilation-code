{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Formula_321_0 AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Formula_321_0')}}

),

Summarize_323 AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Summarize_323')}}

),

Join_329_inner AS (

  SELECT 
    in0.* EXCEPT (`NDOD`),
    in1.*
  
  FROM Summarize_323 AS in0
  INNER JOIN Formula_321_0 AS in1
     ON (in0.NDOD = in1.NDOD)

)

SELECT *

FROM Join_329_inner
