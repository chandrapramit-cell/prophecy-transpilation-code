{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Summarize_12 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Summarize_12')}}

),

Summarize_19 AS (

  SELECT 
    AVG(CountDistinct_MBR_INDV_BE_KEY) AS Avg_CountDistinct_MBR_INDV_BE_KEY,
    STDDEV(CountDistinct_MBR_INDV_BE_KEY) AS StdDev_CountDistinct_MBR_INDV_BE_KEY
  
  FROM Summarize_12 AS in0

)

SELECT *

FROM Summarize_19
