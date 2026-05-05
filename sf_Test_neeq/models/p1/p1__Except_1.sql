{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Except_1 AS (

  SELECT * 
  
  FROM "" AS in0
  
  EXCEPT
  
  SELECT * 
  
  FROM "" AS in1

)

SELECT *

FROM Except_1
