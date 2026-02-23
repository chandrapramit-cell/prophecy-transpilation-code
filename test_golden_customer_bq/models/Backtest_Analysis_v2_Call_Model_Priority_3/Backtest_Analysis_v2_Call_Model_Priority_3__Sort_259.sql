{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_258 AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Filter_258')}}

),

Summarize_257 AS (

  SELECT 
    COUNT(DISTINCT row_id) AS CountDistinct_row_id,
    SUM(`Called in 3`) AS `Sum_Called in 3`,
    AVG(`Called in 3`) AS `Avg_Called in 3`,
    Prediction AS Prediction
  
  FROM Filter_258 AS in0
  
  GROUP BY Prediction

),

Sort_259 AS (

  SELECT * 
  
  FROM Summarize_257 AS in0
  
  ORDER BY Prediction DESC

)

SELECT *

FROM Sort_259
