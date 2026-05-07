{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_41 AS (

  SELECT * 
  
  FROM {{ ref('seed_41')}}

),

TextInput_41_cast AS (

  SELECT 
    "YEAR" AS "YEAR",
    "COUNT" AS "COUNT"
  
  FROM TextInput_41 AS in0

)

SELECT *

FROM TextInput_41_cast
