{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sort_125 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Sort_125')}}

),

Filter_99 AS (

  SELECT * 
  
  FROM Sort_125 AS in0
  
  WHERE (
          (((Avg_TotalRVU <> 0) OR (Avg_TotalRVU IS NULL)) OR (CountDistinct_CLM_ID <> 0))
          OR (CountDistinct_CLM_ID IS NULL)
        )

)

SELECT *

FROM Filter_99
