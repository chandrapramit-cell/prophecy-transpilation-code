{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sort_39 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Sort_39')}}

),

Formula_80_0 AS (

  SELECT 
    CAST(CAST(DATE_DIFF(
      (PARSE_DATE('%Y-%m-%d', CAST(CURRENT_DATE AS STRING))), 
      (PARSE_DATE('%Y-%m-%d', MIN_CLM_SVC_STRT_DT_SK)), 
      MONTH) AS INTEGER) AS INTEGER) AS `Time Since First Diagnosis`,
    *
  
  FROM Sort_39 AS in0

),

Formula_80_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`Time Since First Diagnosis` <= 24)
          THEN '1. 1-2 years            '
        WHEN (`Time Since First Diagnosis` <= 72)
          THEN '2. 3-6 years            '
        WHEN (`Time Since First Diagnosis` <= 120)
          THEN '3. 7-10 years           '
        ELSE '4. Greater than 10 Years'
      END
    ) AS STRING) AS `Time Since First Diagnosis paranthesesOpenCatparanthesesClose`,
    *
  
  FROM Formula_80_0 AS in0

)

SELECT *

FROM Formula_80_1
