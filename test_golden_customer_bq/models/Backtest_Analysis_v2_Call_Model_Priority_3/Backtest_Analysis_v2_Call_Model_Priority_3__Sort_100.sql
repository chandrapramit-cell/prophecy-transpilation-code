{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_93_1 AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Formula_93_1')}}

),

Filter_95 AS (

  SELECT * 
  
  FROM Formula_93_1 AS in0
  
  WHERE (`Recent Call` = CAST('0' AS INT64))

),

Summarize_96 AS (

  SELECT 
    COUNT(DISTINCT row_id) AS CountDistinct_row_id,
    AVG(`Called in 3`) AS `Avg_Called in 3`,
    SUM(`Called in 3`) AS `Sum_Called in 3`,
    Prediction AS Prediction
  
  FROM Filter_95 AS in0
  
  GROUP BY Prediction

),

Sort_100 AS (

  SELECT * 
  
  FROM Summarize_96 AS in0
  
  ORDER BY Prediction DESC

)

SELECT *

FROM Sort_100
