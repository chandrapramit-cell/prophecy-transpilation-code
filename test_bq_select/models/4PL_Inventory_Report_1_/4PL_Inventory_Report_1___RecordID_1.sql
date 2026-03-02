{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH RecordID_1 AS (

  {{
    prophecy_basics.RecordID(
      [''], 
      'incremental_id', 
      'RecordID', 
      'string', 
      6, 
      1000, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

)

SELECT *

FROM RecordID_1
