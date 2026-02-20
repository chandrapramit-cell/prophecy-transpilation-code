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

Summarize_826 AS (

  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    AdvancedCKD_Risk AS AdvancedCKD_Risk
  
  FROM Union_817 AS in0
  
  GROUP BY AdvancedCKD_Risk

)

SELECT *

FROM Summarize_826
