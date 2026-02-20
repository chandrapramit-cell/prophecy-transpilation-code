{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Summarize_173 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Summarize_173')}}

),

Summarize_174 AS (

  SELECT MIN(CountDistinct_YearMonth) AS Min_CountDistinct_YearMonth
  
  FROM Summarize_173 AS in0

)

SELECT *

FROM Summarize_174
