{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH LockInFilter_128 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report_1___LockInFilter_128')}}

),

generate_incremental_recordid AS (

  {#Generates sequential record IDs for data from LockInFilter_128, enabling unique, table-level identifiers for new records.#}
  {{
    prophecy_basics.RecordID(
      ['LockInFilter_128'], 
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

FROM generate_incremental_recordid
