{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_2 AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Filter_2')}}

),

Summarize_8 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    YMD AS YMD
  
  FROM Filter_2 AS in0
  
  GROUP BY YMD

),

Sort_9 AS (

  SELECT * 
  
  FROM Summarize_8 AS in0
  
  ORDER BY YMD ASC

)

SELECT *

FROM Sort_9
