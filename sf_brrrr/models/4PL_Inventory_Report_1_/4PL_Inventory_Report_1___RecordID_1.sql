{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_119_0 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report_1___Formula_119_0')}}

),

RecordID_1 AS (

  {{
    prophecy_basics.RecordID(
      ['Formula_119_0'], 
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
