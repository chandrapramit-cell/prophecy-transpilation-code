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

Filter_1656 AS (

  SELECT * 
  
  FROM Union_817 AS in0
  
  WHERE (`Member Individual Business Entity Key` = '10000010052')

)

SELECT *

FROM Filter_1656
