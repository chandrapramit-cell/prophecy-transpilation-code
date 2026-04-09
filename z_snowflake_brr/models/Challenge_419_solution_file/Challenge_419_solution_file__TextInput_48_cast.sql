{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_48 AS (

  SELECT * 
  
  FROM {{ ref('seed_48')}}

),

TextInput_48_cast AS (

  SELECT 
    CAST("BASKET LIST" AS STRING) AS "BASKET LIST",
    "PERCENT INCREASE 2021 TO 2022" AS "PERCENT INCREASE 2021 TO 2022",
    "PERCENT INCREASE 2022 TO 2023" AS "PERCENT INCREASE 2022 TO 2023"
  
  FROM TextInput_48 AS in0

)

SELECT *

FROM TextInput_48_cast
