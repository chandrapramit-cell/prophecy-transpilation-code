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

Summarize_152 AS (

  SELECT 
    MIN(`Standardized RVU`) AS `Min_Standardized RVU`,
    MAX(`Standardized RVU`) AS `Max_Standardized RVU`,
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    Bucket AS Bucket
  
  FROM Join_146_inner_formula_to_Formula_148_1 AS in0
  
  GROUP BY 
    MBR_AGE_RANGE, Bucket

),

Sort_153 AS (

  SELECT * 
  
  FROM Summarize_152 AS in0
  
  ORDER BY MBR_AGE_RANGE ASC, Bucket ASC

)

SELECT *

FROM Sort_153
