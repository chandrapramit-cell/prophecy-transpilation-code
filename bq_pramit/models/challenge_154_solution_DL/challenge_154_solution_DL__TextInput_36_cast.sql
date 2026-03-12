{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_36 AS (

  SELECT * 
  
  FROM {{ ref('seed_36')}}

),

TextInput_36_cast AS (

  SELECT 
    Scenario AS Scenario,
    CAST(`Option 1` AS STRING) AS `Option 1`,
    CAST(`Option 2` AS STRING) AS `Option 2`
  
  FROM TextInput_36 AS in0

)

SELECT *

FROM TextInput_36_cast
