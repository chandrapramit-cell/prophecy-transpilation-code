{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Database__LOADI_292 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3656', 'Database__LOADI_292') }}

),

Summarize_379 AS (

  SELECT DISTINCT NDOD AS NDOD
  
  FROM Database__LOADI_292 AS in0

)

SELECT *

FROM Summarize_379
