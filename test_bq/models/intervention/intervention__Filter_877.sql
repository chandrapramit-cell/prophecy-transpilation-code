{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Union_817 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_817')}}

),

Filter_877 AS (

  SELECT * 
  
  FROM Union_817 AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_877
