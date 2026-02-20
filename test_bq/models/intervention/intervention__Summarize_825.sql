{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_822_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_822_left_UnionLeftOuter')}}

),

Summarize_825 AS (

  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    AdvancedCKD_Risk AS AdvancedCKD_Risk
  
  FROM Join_822_left_UnionLeftOuter AS in0
  
  GROUP BY AdvancedCKD_Risk

)

SELECT *

FROM Summarize_825
