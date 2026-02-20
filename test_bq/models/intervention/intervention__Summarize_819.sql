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

Summarize_819 AS (

  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    `ED Prediction Score` AS `ED Prediction Score`
  
  FROM Union_817 AS in0
  
  GROUP BY `ED Prediction Score`

)

SELECT *

FROM Summarize_819
