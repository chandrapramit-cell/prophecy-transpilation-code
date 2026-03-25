{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_70 AS (

  SELECT * 
  
  FROM {{ ref('seed_70')}}

),

TextInput_70_cast AS (

  SELECT CAST(COMPANY_CODE AS VARstring) AS COMPANY_CODE
  
  FROM TextInput_70 AS in0

)

SELECT *

FROM TextInput_70_cast
