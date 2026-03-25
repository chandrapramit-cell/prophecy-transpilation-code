{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_138 AS (

  SELECT * 
  
  FROM {{ ref('seed_138')}}

),

TextInput_138_cast AS (

  SELECT FILENAME AS FILENAME
  
  FROM TextInput_138 AS in0

),

AlteryxSelect_139 AS (

  SELECT CAST(FILENAME AS STRING) AS "FILE NAME"
  
  FROM TextInput_138_cast AS in0

)

SELECT *

FROM AlteryxSelect_139
