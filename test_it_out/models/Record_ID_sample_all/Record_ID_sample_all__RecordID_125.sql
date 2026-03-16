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

RecordID_125 AS (

  SELECT 
    *,
    (row_number() OVER (ORDER BY 1)) + (-100) AS `Record ID Last Column`
  
  FROM TextInput_123_cast

)

SELECT *

FROM RecordID_125
