{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_57_inner AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Join_57_inner')}}

),

Summarize_4 AS (

  SELECT 
    SUM(CAST(`Blue Connect Household` AS NUMERIC)) AS `Sum_Blue Connect Household`,
    YMD AS YMD
  
  FROM Join_57_inner AS in0
  
  GROUP BY YMD

),

Sort_5 AS (

  SELECT * 
  
  FROM Summarize_4 AS in0
  
  ORDER BY YMD ASC

)

SELECT *

FROM Sort_5
