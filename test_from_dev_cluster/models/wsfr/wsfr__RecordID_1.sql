{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH Table_0 AS (

  SELECT * 
  
  FROM {{ ref('sf')}}

),

RecordID_1 AS (

  {{
    prophecy_basics.RecordID(
      ['Table_0'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      10, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

)

SELECT *

FROM RecordID_1
