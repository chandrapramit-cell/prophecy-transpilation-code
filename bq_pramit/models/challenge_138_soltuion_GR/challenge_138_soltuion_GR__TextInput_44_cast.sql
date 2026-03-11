{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_44 AS (

  SELECT * 
  
  FROM {{ ref('seed_44')}}

),

TextInput_44_cast AS (

  SELECT 
    CAST(Team AS STRING) AS Team,
    Appearances AS Appearances,
    CAST(`Winning Years` AS STRING) AS `Winning Years`,
    CAST(`Winning Percent` AS STRING) AS `Winning Percent`
  
  FROM TextInput_44 AS in0

)

SELECT *

FROM TextInput_44_cast
