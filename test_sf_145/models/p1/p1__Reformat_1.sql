{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Table_0 AS (

  SELECT * 
  
  FROM {{ ref('ABC')}}

),

Reformat_1 AS (

  SELECT 
    1 AS "ABC_DEF  ",
    *
  
  FROM Table_0 AS in0

)

SELECT *

FROM Reformat_1
