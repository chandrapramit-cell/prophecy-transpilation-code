{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_42 AS (

  SELECT * 
  
  FROM {{ ref('seed_42')}}

),

TextInput_42_cast AS (

  SELECT CAST(Solution AS STRING) AS Solution
  
  FROM TextInput_42 AS in0

)

SELECT *

FROM TextInput_42_cast
