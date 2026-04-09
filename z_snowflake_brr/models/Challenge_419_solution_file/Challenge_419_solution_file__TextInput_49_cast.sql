{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_49 AS (

  SELECT * 
  
  FROM {{ ref('seed_49')}}

),

TextInput_49_cast AS (

  SELECT 
    "AVG_PERCENT INCREASE 2023" AS "AVG_PERCENT INCREASE 2023",
    "AVG_BASKET PRICE 2023" AS "AVG_BASKET PRICE 2023",
    "FUNDRAISING GOAL 2024" AS "FUNDRAISING GOAL 2024"
  
  FROM TextInput_49 AS in0

)

SELECT *

FROM TextInput_49_cast
