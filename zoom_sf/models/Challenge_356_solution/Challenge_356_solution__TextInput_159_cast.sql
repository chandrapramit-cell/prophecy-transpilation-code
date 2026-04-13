{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_159 AS (

  SELECT * 
  
  FROM {{ ref('seed_159')}}

),

TextInput_159_cast AS (

  SELECT 
    CAST(WORD AS STRING) AS WORD,
    LENGTH AS LENGTH
  
  FROM TextInput_159 AS in0

)

SELECT *

FROM TextInput_159_cast
