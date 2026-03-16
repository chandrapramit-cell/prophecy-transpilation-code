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

RecordID_124 AS (

  SELECT 
    *,
    (row_number() OVER (ORDER BY 1)) + (9) AS `Record ID`
  
  FROM TextInput_123_cast

)

SELECT *

FROM RecordID_124
