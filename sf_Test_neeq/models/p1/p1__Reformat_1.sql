{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH seed_403_459 AS (

  SELECT * 
  
  FROM {{ ref('seed_403_459')}}

),

Reformat_1 AS (

  SELECT * 
  
  FROM seed_403_459 AS in0

)

SELECT *

FROM Reformat_1
