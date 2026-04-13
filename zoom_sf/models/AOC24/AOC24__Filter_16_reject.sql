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

Filter_16_reject AS (

  SELECT * 
  
  FROM Filter_17 AS in0
  
  WHERE (
          (
            NOT(
              ((("X-INT" >= 200000000000000) AND ("X-INT" <= 400000000000000)) AND ("Y-INT" >= 200000000000000))
              AND ("Y-INT" <= 400000000000000))
          )
          OR (
               (
                 ((("X-INT" >= 200000000000000) AND ("X-INT" <= 400000000000000)) AND ("Y-INT" >= 200000000000000))
                 AND ("Y-INT" <= 400000000000000)
               ) IS NULL
             )
        )

)

SELECT *

FROM Filter_16_reject
