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

Summarize_94 AS (

  SELECT 
    AVG(`Recent Call`) AS `Avg_Recent Call`,
    COUNT(DISTINCT row_id) AS CountDistinct_row_id,
    AVG(CAST(`Unweighted Number of Calls` AS NUMERIC)) AS `Avg_Unweighted Number of Calls`,
    Prediction AS Prediction
  
  FROM Formula_93_1 AS in0
  
  GROUP BY Prediction

),

Sort_99 AS (

  SELECT * 
  
  FROM Summarize_94 AS in0
  
  ORDER BY Prediction DESC

)

SELECT *

FROM Sort_99
