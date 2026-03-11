{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_11 AS (

  SELECT * 
  
  FROM {{ ref('seed_11')}}

),

TextInput_11_cast AS (

  SELECT 
    CAST(Label AS STRING) AS Label,
    CAST(`Spatial Object Final` AS STRING) AS `Spatial Object Final`
  
  FROM TextInput_11 AS in0

)

SELECT *

FROM TextInput_11_cast
