{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TOTAL_INF_predi_171 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'TOTAL_INF_predi_171_ref') }}

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
