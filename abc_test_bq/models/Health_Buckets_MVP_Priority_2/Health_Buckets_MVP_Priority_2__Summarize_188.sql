{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_146_inner_formula_to_Formula_148_1 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Join_146_inner_formula_to_Formula_148_1')}}

),

Summarize_188 AS (

  SELECT 
    MIN(`Standardized RVU`) AS `Min_Standardized RVU`,
    MAX(`Standardized RVU`) AS `Max_Standardized RVU`,
    Bucket AS Bucket
  
  FROM Join_146_inner_formula_to_Formula_148_1 AS in0
  
  GROUP BY Bucket

)

SELECT *

FROM Summarize_188
