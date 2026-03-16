{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_123_cast AS (

  SELECT *
  
  FROM {{ ref('Record_ID_sample_all__TextInput_123_cast')}}

),

generate_incremental_id AS (

  {#Generates unique incremental record IDs for the specified table, ensuring new rows receive a sequential identifier.#}
  {{
    prophecy_basics.RecordID(
      ['TextInput_123_cast'], 
      'incremental_id', 
      '`Record ID Last Column`', 
      'integer', 
      6, 
      -100, 
      'tableLevel', 
      'last_column', 
      [], 
      []
    )
  }}

)

SELECT *

FROM generate_incremental_id
