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

Summarize_108 AS (

  SELECT 
    COUNT(DISTINCT row_id) AS CountDistinct_row_id,
    AVG(`Called in 3`) AS `Avg_Called in 3`,
    AVG(Prediction) AS Avg_Prediction,
    STDDEV(Prediction) AS StdDev_Prediction,
    `Unweighted Number of Calls` AS `Unweighted Number of Calls`
  
  FROM Formula_93_1 AS in0
  
  GROUP BY `Unweighted Number of Calls`

),

Sort_110 AS (

  SELECT * 
  
  FROM Summarize_108 AS in0
  
  ORDER BY `Unweighted Number of Calls` DESC

)

SELECT *

FROM Sort_110
