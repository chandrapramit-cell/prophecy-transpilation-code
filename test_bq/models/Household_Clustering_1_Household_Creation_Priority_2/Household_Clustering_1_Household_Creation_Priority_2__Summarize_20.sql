{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_4 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Unique_4')}}

),

Summarize_20 AS (

  SELECT 
    COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY,
    COUNT(DISTINCT SUB_ID) AS CountDistinct_SUB_ID
  
  FROM Unique_4 AS in0

)

SELECT *

FROM Summarize_20
