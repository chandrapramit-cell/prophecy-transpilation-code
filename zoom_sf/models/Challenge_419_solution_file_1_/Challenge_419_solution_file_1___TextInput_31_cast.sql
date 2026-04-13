{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_31 AS (

  SELECT * 
  
  FROM {{ ref('seed_31')}}

),

TextInput_31_cast AS (

  SELECT 
    CAST(TREAT AS STRING) AS TREAT,
    PRICE AS PRICE,
    "YEAR" AS YEAR
  
  FROM TextInput_31 AS in0

)

SELECT *

FROM TextInput_31_cast
