{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sample_143 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Sample_143')}}

),

Sample_142 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Sample_142')}}

),

Join_155_inner AS (

  SELECT 
    in1.MBR_INDV_BE_KEY AS Right_MBR_INDV_BE_KEY,
    in1.YearMonth AS Right_YearMonth,
    in1.Avg_TotalRVU AS Right_Avg_TotalRVU,
    in0.* EXCEPT (`MBR_AGE_RANGE`),
    in1.* EXCEPT (`MBR_INDV_BE_KEY`, `YearMonth`, `Avg_TotalRVU`)
  
  FROM Sample_142 AS in0
  INNER JOIN Sample_143 AS in1
     ON (in0.MBR_INDV_BE_KEY = in1.MBR_INDV_BE_KEY)

)

SELECT *

FROM Join_155_inner
