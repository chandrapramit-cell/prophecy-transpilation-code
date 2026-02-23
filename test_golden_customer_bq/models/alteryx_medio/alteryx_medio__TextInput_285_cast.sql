{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_285 AS (

  SELECT * 
  
  FROM {{ ref('seed_285')}}

),

TextInput_285_cast AS (

  SELECT 
    CAST(db AS STRING) AS db,
    CAST(schema AS STRING) AS schema,
    CAST(table_name AS STRING) AS table_name
  
  FROM TextInput_285 AS in0

)

SELECT *

FROM TextInput_285_cast
