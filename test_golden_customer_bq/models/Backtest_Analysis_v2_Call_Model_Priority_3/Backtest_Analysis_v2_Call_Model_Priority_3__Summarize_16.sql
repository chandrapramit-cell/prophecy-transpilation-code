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

Summarize_16 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    `Max_April Caller` AS `Max_April Caller`
  
  FROM Filter_2 AS in0
  
  GROUP BY `Max_April Caller`

)

SELECT *

FROM Summarize_16
