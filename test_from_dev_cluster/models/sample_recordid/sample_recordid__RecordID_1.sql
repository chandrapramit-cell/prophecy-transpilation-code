{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH Table_1 AS (

  SELECT * 
  
  FROM {{ ref('sffgfbweg')}}

),

RecordID_1 AS (

  {{
    prophecy_basics.RecordID(
      ['Table_1'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

)

SELECT *

FROM RecordID_1
