{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_782_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_782_0')}}

),

Summarize_786 AS (

  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    `ED Prediction Score` AS `ED Prediction Score`
  
  FROM Formula_782_0 AS in0
  
  GROUP BY `ED Prediction Score`

)

SELECT *

FROM Summarize_786
