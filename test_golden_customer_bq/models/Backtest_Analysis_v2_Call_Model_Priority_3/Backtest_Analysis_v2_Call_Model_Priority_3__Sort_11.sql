{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_3_inner AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Join_3_inner')}}

),

Summarize_10 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    YMD AS YMD
  
  FROM Join_3_inner AS in0
  
  GROUP BY YMD

),

Sort_11 AS (

  SELECT * 
  
  FROM Summarize_10 AS in0
  
  ORDER BY YMD ASC

)

SELECT *

FROM Sort_11
