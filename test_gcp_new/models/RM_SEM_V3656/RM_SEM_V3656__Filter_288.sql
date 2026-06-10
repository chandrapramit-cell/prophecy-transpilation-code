{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Database__LOADI_283 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3656', 'Database__LOADI_283') }}

),

Summarize_285 AS (

  SELECT 
    SUM(`SUM(REV.LEG_PAX_CNT)`) AS TTL_BKGS,
    LEG_NDOD AS LEG_NDOD
  
  FROM Database__LOADI_283 AS in0
  
  GROUP BY LEG_NDOD

),

Filter_288 AS (

  SELECT * 
  
  FROM Summarize_285 AS in0
  
  WHERE (TTL_BKGS > 0)

)

SELECT *

FROM Filter_288
