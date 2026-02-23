{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_34 AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Unique_34')}}

),

Summarize_36 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    `Blue Connect Household` AS `Blue Connect Household`
  
  FROM Unique_34 AS in0
  
  GROUP BY `Blue Connect Household`

)

SELECT *

FROM Summarize_36
