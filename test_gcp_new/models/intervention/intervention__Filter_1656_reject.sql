{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Union_817 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_817')}}

),

Filter_1656_reject AS (

  SELECT * 
  
  FROM Union_817 AS in0
  
  WHERE (
          (
            NOT(
              `Member Individual Business Entity Key` = '10000010052')
          )
          OR ((`Member Individual Business Entity Key` = '10000010052') IS NULL)
        )

)

SELECT *

FROM Filter_1656_reject
