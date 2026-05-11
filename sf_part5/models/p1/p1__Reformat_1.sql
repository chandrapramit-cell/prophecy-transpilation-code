{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Table_0 AS (

  SELECT * 
  
  FROM {{ ref('sf')}}

),

Reformat_1 AS (

  SELECT * 
  
  FROM Table_0 AS in0

)

SELECT *

FROM Reformat_1
