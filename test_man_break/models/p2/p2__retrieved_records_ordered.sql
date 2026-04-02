{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH table_test AS (

  SELECT * 
  
  FROM {{ source('avpreet_qa_avpreettables', 'table_test') }}

),

RecordID_1 AS (

  {{
    prophecy_basics.RecordID(
      ['table_test'], 
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

),

retrieved_records_ordered AS (

  {#Fetches records from RecordID_1 in ascending order by ID for ordered results.#}
  SELECT * 
  
  FROM RecordID_1 AS in0
  
  ORDER BY ID ASC

)

SELECT *

FROM retrieved_records_ordered
