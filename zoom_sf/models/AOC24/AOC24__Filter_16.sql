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
  
  WHERE (
          ((("X-INT" >= 200000000000000) AND ("X-INT" <= 400000000000000)) AND ("Y-INT" >= 200000000000000))
          AND ("Y-INT" <= 400000000000000)
        )

)

SELECT *

FROM Filter_16
