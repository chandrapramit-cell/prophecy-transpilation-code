{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH table_3124_Engine_Records_macro_ip AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_3124_Engine_Records_macro_ip') }}

),

RecordID_110_3124 AS (

  {{
    prophecy_basics.RecordID(
      ['table_3124_Engine_Records_macro_ip'], 
      'incremental_id', 
      'MacroRecordID', 
      'integer', 
      6, 
      1, 
      'groupLevel', 
      'first_column', 
      [], 
      [
        { 'expression': { 'expression': 'RecordID' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'prophecy_recordId_564' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'ManualRecordID' }, 'sortType': 'asc' }
      ]
    )
  }}

)

SELECT *

FROM RecordID_110_3124
