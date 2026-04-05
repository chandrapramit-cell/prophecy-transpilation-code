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

generate_record_id AS (

  {#Generates a unique, table-level Record ID starting at 9 for the first column in the specified table.\#}
  {{
    prophecy_basics.RecordID(
      ['TextInput_123_cast'], 
      'incremental_id', 
      '`Record ID`', 
      'string', 
      6, 
      9, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

)

SELECT *

FROM generate_record_id
