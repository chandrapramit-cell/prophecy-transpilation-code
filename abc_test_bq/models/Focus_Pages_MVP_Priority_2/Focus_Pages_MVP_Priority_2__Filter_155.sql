{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_102_1 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Formula_102_1')}}

),

Filter_155 AS (

  SELECT * 
  
  FROM Formula_102_1 AS in0
  
  WHERE (Trimester <> 'Error')

)

SELECT *

FROM Filter_155
