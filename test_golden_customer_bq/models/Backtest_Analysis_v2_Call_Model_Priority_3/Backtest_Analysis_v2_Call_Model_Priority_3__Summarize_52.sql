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

Filter_56 AS (

  SELECT * 
  
  FROM Filter_2 AS in0
  
  WHERE (YMD < '2023-02-01')

),

Summarize_51 AS (

  SELECT 
    MAX(`Called in 3`) AS `Max_Called in 3`,
    Household AS Household
  
  FROM Filter_56 AS in0
  
  GROUP BY Household

),

Summarize_52 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    `Max_Called in 3` AS `Max_Called in 3`
  
  FROM Summarize_51 AS in0
  
  GROUP BY `Max_Called in 3`

)

SELECT *

FROM Summarize_52
