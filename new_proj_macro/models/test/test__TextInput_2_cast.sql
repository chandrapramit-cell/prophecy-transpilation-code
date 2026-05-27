{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_2 AS (

  SELECT * 
  
  FROM {{ ref('seed_test_2')}}

),

TextInput_2_cast AS (

  SELECT 
    CAST(Product AS string) AS Product,
    CAST(`Box Size` AS INTEGER) AS `Box Size`
  
  FROM TextInput_2 AS in0

)

SELECT *

FROM TextInput_2_cast
