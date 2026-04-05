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

incremental_recordid_group_region AS (

  {#Generates unique, sequential IDs for rows grouped by Region to support traceable records.#}
  {{
    prophecy_basics.RecordID(
      ['TextInput_123_cast'], 
      'incremental_id', 
      '`Grouped Record ID`', 
      'integer', 
      6, 
      1, 
      'groupLevel', 
      'last_column', 
      ['Region', 'Gender'], 
      []
    )
  }}

)

SELECT *

FROM incremental_recordid_group_region
