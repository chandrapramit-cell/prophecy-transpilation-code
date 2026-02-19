{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_99_reject AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Filter_99_reject')}}

),

Summarize_190 AS (

  SELECT COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY
  
  FROM Filter_99_reject AS in0

)

SELECT *

FROM Summarize_190
