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

  SELECT FileName AS FileName
  
  FROM TextInput_138 AS in0

),

AlteryxSelect_139 AS (

  SELECT CAST(FileName AS VARstring) AS `File Name`
  
  FROM TextInput_138_cast AS in0

)

SELECT *

FROM AlteryxSelect_139
