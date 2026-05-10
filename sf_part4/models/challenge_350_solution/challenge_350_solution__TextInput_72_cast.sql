{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_72 AS (

  SELECT * 
  
  FROM {{ ref('seed_72')}}

),

TextInput_72_cast AS (

  SELECT 
    CAST("LANGUAGE" AS STRING) AS "LANGUAGE",
    CAST("TYPE" AS STRING) AS "TYPE",
    CAST("HOLIDAY MESSAGES" AS STRING) AS "HOLIDAY MESSAGES"
  
  FROM TextInput_72 AS in0

)

SELECT *

FROM TextInput_72_cast
