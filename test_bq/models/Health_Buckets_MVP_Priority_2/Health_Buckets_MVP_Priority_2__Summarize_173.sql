{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TOTAL_INF_predi_171 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Health_Buckets_MVP_Priority_2', 'TOTAL_INF_predi_171') }}

),

Summarize_173 AS (

  SELECT 
    COUNT(DISTINCT YearMonth) AS CountDistinct_YearMonth,
    `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`
  
  FROM TOTAL_INF_predi_171 AS in0
  
  GROUP BY `Member Individual Business Entity Key`

)

SELECT *

FROM Summarize_173
