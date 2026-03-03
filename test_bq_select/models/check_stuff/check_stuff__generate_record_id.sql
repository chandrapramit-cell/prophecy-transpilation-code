{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH random AS (

  SELECT * 
  
  FROM {{ ref('random')}}

),

generate_record_id AS (

  {#Generates sequential record IDs for new rows to ensure unique, orderly identifiers in the main table.#}
  {{
    prophecy_basics.RecordID(
      ['random'], 
      'uuid', 
      'RecordID', 
      'integer', 
      6, 
      1000, 
      'tableLevel', 
      'last_column', 
      [], 
      []
    )
  }}

)

SELECT *

FROM generate_record_id
