{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH seed_61 AS (

  SELECT * 
  
  FROM {{ ref('seed_61')}}

),

Union_1 AS (

  SELECT * 
  
  FROM seed_61 AS in0
  
  UNION ALL
  
  SELECT * 
  
  FROM seed_61 AS in1

)

SELECT *

FROM Union_1
