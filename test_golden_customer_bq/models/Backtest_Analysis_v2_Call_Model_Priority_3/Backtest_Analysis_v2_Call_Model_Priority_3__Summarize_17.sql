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

Summarize_17 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    `Max_April Caller` AS `Max_April Caller`
  
  FROM Join_57_inner AS in0
  
  GROUP BY `Max_April Caller`

)

SELECT *

FROM Summarize_17
