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

Summarize_820 AS (

  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    `Prediction Score` AS `Prediction Score`
  
  FROM Union_817 AS in0
  
  GROUP BY `Prediction Score`

)

SELECT *

FROM Summarize_820
