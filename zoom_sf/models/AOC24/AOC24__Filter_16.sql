{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Filter_17 AS (

  SELECT *
  
  FROM {{ ref('AOC24__Filter_17')}}

),

Filter_16 AS (

  SELECT * 
  
  FROM Filter_17 AS in0
  
  WHERE (((("X-INT" >= 2.0E14D) AND ("X-INT" <= 4.0E14D)) AND ("Y-INT" >= 2.0E14D)) AND ("Y-INT" <= 4.0E14D))

)

SELECT *

FROM Filter_16
