{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_23_1 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__Formula_23_1')}}

),

Filter_24 AS (

  SELECT * 
  
  FROM Formula_23_1 AS in0
  
  WHERE CAST(`Type Test` AS BOOLEAN)

)

SELECT *

FROM Filter_24
