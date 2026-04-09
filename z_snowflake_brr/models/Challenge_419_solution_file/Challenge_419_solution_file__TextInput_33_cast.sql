{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_33 AS (

  SELECT * 
  
  FROM {{ ref('seed_33')}}

),

TextInput_33_cast AS (

  SELECT 
    CAST("BASKET LIST" AS STRING) AS "BASKET LIST",
    "PURCHASE YEAR" AS "PURCHASE YEAR",
    CAST(TREAT AS STRING) AS TREAT,
    CAST("STUFFED ANIMAL" AS STRING) AS "STUFFED ANIMAL"
  
  FROM TextInput_33 AS in0

)

SELECT *

FROM TextInput_33_cast
