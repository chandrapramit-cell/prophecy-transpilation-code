{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_372 AS (

  SELECT *
  
  FROM {{ ref('p1__AlteryxSelect_372')}}

),

AlteryxSelect_384 AS (

  SELECT ROWID AS "0"
  
  FROM AlteryxSelect_372 AS in0

),

SelectRecords_386_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['AlteryxSelect_384'], 
      'incremental_id', 
      'ROW_NUMBER', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

SelectRecords_386 AS (

  SELECT * 
  
  FROM SelectRecords_386_rowNumber AS in0
  
  WHERE (ROW_NUMBER = 0)

),

SelectRecords_386_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_386 AS in0

)

SELECT *

FROM SelectRecords_386_cleanup_0
