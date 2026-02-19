{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_99 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Filter_99')}}

),

Summarize_104 AS (

  SELECT COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY
  
  FROM Filter_99 AS in0

)

SELECT *

FROM Summarize_104
