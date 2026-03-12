{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_48 AS (

  SELECT * 
  
  FROM {{ ref('seed_48')}}

),

TextInput_48_cast AS (

  SELECT `People Needed` AS `People Needed`
  
  FROM TextInput_48 AS in0

)

SELECT *

FROM TextInput_48_cast
