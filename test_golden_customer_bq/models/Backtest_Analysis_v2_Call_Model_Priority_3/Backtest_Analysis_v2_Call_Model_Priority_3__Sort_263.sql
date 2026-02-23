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

Summarize_262 AS (

  SELECT 
    AVG(Prediction) AS Avg_Prediction,
    AVG(`Called in 3`) AS `Avg_Called in 3`,
    COUNT(DISTINCT row_id) AS CountDistinct_row_id,
    SUM(`Called in 3`) AS `Sum_Called in 3`,
    AVG(`Recent Call`) AS `Avg_Recent Call`,
    AVG(CAST(`Unweighted Number of Calls` AS NUMERIC)) AS `Avg_Unweighted Number of Calls`,
    `Prediction Cat` AS `Prediction Cat`
  
  FROM Filter_258 AS in0
  
  GROUP BY `Prediction Cat`

),

Sort_263 AS (

  SELECT * 
  
  FROM Summarize_262 AS in0
  
  ORDER BY `Prediction Cat` ASC

)

SELECT *

FROM Sort_263
