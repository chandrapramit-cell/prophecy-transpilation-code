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

Summarize_139 AS (

  SELECT 
    DISTINCT MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    YearMonth AS YearMonth,
    TotalRVU AS TotalRVU
  
  FROM Filter_99 AS in0

),

Sample_143 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Sample_143')}}

),

Join_138_inner AS (

  SELECT 
    in0.MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    in0.* EXCEPT (`MBR_INDV_BE_KEY`),
    in1.* EXCEPT (`MBR_INDV_BE_KEY`, `YearMonth`)
  
  FROM Sample_143 AS in0
  INNER JOIN Summarize_139 AS in1
     ON ((t.MBR_INDV_BE_KEY0 = t0.MBR_INDV_BE_KEY0) AND (t.YearMonth = t0.YearMonth0))

),

Sort_141 AS (

  SELECT * 
  
  FROM Join_138_inner AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, YearMonth ASC

)

SELECT *

FROM Sort_141
