{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RecordID_1 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM in0

)

SELECT *

FROM RecordID_1
