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

Summarize_14 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    YMD AS YMD,
    `Called in 3` AS `Called in 3`
  
  FROM Filter_2 AS in0
  
  GROUP BY 
    YMD, `Called in 3`

),

Sort_15 AS (

  SELECT * 
  
  FROM Summarize_14 AS in0
  
  ORDER BY YMD ASC, `Called in 3` ASC

)

SELECT *

FROM Sort_15
