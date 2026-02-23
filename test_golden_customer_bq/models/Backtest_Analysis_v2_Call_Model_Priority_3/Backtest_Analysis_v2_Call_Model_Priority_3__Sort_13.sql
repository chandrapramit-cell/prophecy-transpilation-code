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

Summarize_12 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    YMD AS YMD,
    `Called in 3` AS `Called in 3`
  
  FROM Join_3_inner AS in0
  
  GROUP BY 
    YMD, `Called in 3`

),

Sort_13 AS (

  SELECT * 
  
  FROM Summarize_12 AS in0
  
  ORDER BY YMD ASC, `Called in 3` ASC

)

SELECT *

FROM Sort_13
