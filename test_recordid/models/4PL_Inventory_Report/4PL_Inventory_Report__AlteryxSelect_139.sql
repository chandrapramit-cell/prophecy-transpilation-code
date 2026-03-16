{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TextInput_138 AS (

  SELECT * 
  
  FROM {{ ref('seed_138')}}

),

TextInput_138_cast AS (

  SELECT CAST(FileName AS BOOLEAN) AS FileName
  
  FROM TextInput_138 AS in0

),

AlteryxSelect_139 AS (

  SELECT 
    CAST(FileName AS string) AS `File Name`,
    * EXCEPT (`FileName`)
  
  FROM TextInput_138_cast AS in0

)

SELECT *

FROM AlteryxSelect_139
