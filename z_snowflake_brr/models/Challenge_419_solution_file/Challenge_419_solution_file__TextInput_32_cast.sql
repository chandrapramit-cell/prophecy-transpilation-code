{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_32 AS (

  SELECT * 
  
  FROM {{ ref('seed_32')}}

),

TextInput_32_cast AS (

  SELECT 
    CAST("STUFFED ANIMAL" AS STRING) AS "STUFFED ANIMAL",
    PRICE AS PRICE,
    "YEAR" AS YEAR
  
  FROM TextInput_32 AS in0

)

SELECT *

FROM TextInput_32_cast
