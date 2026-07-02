{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH NewMachineSample_xlsx_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'NewMachineSample_xlsx_1') }}

),

RecordID_1 AS (

  {{
    prophecy_basics.RecordID(
      ['NewMachineSample_xlsx_1'], 
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
