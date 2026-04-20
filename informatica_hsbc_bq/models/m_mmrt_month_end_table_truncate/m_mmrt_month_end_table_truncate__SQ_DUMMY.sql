{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SQ_DUMMY AS (

  SELECT 'X'
  
  FROM DUAL

)

SELECT *

FROM SQ_DUMMY
