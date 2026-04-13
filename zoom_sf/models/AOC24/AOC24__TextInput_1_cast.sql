{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_1')}}

),

TextInput_1_cast AS (

  SELECT CAST(FIELD1 AS STRING) AS FIELD1
  
  FROM TextInput_1 AS in0

)

SELECT *

FROM TextInput_1_cast
