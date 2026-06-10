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

Sort_377 AS (

  SELECT * 
  
  FROM Summarize_370 AS in0
  
  ORDER BY (ENCODE(CAST(Count AS string), 'utf-8')) ASC

)

SELECT *

FROM Sort_377
