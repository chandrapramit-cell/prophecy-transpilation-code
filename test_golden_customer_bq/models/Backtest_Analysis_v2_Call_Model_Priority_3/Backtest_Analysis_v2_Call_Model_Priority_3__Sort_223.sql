{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_112_inner AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__Join_112_inner')}}

),

Filter_219 AS (

  SELECT * 
  
  FROM Join_112_inner AS in0
  
  WHERE (YMD < '2023-02-01')

),

AlteryxSelect_220 AS (

  SELECT 
    CAST(`Number of Calls` AS INT64) AS `Number of Calls`,
    CAST(`Months Until Call` AS INT64) AS `Months Until Call`,
    CAST(`Called in 3` AS INT64) AS `Called in 3`,
    * EXCEPT (`Number of Calls`, `Months Until Call`, `Called in 3`)
  
  FROM Filter_219 AS in0

),

Formula_221_0 AS (

  SELECT 
    (
      CASE
        WHEN (CAST(`Unweighted Number of Calls` AS INT64) > 0)
          THEN 1
        ELSE 0
      END
    ) AS `Recent Call`,
    (
      (
        CASE
          WHEN (((Prediction / 0.1) < 0) AND (((Prediction / 0.1) - FLOOR((Prediction / 0.1))) = 0.5))
            THEN CEIL((Prediction / 0.1))
          ELSE ROUND((Prediction / 0.1))
        END
      )
      * 0.1
    ) AS Prediction,
    * EXCEPT (`prediction`)
  
  FROM AlteryxSelect_220 AS in0

),

Summarize_222 AS (

  SELECT 
    SUM(`Called in 3`) AS `Sum_Called in 3`,
    AVG(`Called in 3`) AS `Avg_Called in 3`,
    Prediction AS Prediction
  
  FROM Formula_221_0 AS in0
  
  GROUP BY Prediction

),

Sort_223 AS (

  SELECT * 
  
  FROM Summarize_222 AS in0
  
  ORDER BY Prediction DESC

)

SELECT *

FROM Sort_223
