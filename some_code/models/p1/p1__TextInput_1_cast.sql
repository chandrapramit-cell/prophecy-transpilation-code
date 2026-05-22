{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_test_1')}}

),

TextInput_1_cast AS (

  SELECT CAST(USERID AS string) AS USERID
  
  FROM TextInput_1 AS in0

)

SELECT *

FROM TextInput_1_cast
