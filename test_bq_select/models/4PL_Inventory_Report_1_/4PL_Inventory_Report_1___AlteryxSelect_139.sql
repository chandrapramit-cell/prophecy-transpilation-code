{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_138 AS (

  SELECT * 
  
  FROM {{ ref('seed_138')}}

),

TextInput_138_cast AS (

  SELECT FileName AS FileName
  
  FROM TextInput_138 AS in0

),

AlteryxSelect_139 AS (

  SELECT 
    CAST(FileName AS STRING) AS `File Name`,
    *
  
  FROM TextInput_138_cast

)

SELECT *

FROM AlteryxSelect_139
