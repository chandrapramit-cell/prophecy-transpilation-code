{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_1018 AS (

  SELECT *
  
  FROM {{ ref('intervention__Unique_1018')}}

),

Filter_1019 AS (

  SELECT * 
  
  FROM Unique_1018 AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_1019
