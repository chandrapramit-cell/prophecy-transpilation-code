{{
  config({    
    "materialized": "ephemeral",
    "database": "rohit",
    "schema": "default"
  })
}}

WITH TextInput_218 AS (

  SELECT * 
  
  FROM {{ ref('seed_218')}}

),

TextInput_218_cast AS (

  SELECT 
    CAST(Field_Target AS string) AS Field_Target,
    CAST(Field_Source AS string) AS Field_Source
  
  FROM TextInput_218 AS in0

)

SELECT *

FROM TextInput_218_cast
