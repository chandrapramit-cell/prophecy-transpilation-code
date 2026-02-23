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

Filter_260 AS (

  SELECT * 
  
  FROM Join_112_inner AS in0
  
  WHERE (YMD = '2022-12-01')

),

AlteryxSelect_92 AS (

  SELECT 
    CAST(`Number of Calls` AS INT64) AS `Number of Calls`,
    CAST(`Months Until Call` AS INT64) AS `Months Until Call`,
    CAST(`Called in 3` AS INT64) AS `Called in 3`,
    * EXCEPT (`Number of Calls`, `Months Until Call`, `Called in 3`)
  
  FROM Filter_260 AS in0

),

Formula_93_0 AS (

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
          WHEN (((Prediction / 0.05) < 0) AND (((Prediction / 0.05) - FLOOR((Prediction / 0.05))) = 0.5))
            THEN CEIL((Prediction / 0.05))
          ELSE ROUND((Prediction / 0.05))
        END
      )
      * 0.05
    ) AS Prediction_05,
    *
  
  FROM AlteryxSelect_92 AS in0

),

Formula_93_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Prediction_05 >= 0.75)
          THEN '1. High  '
        WHEN (Prediction_05 >= 0.4)
          THEN '2. Medium'
        ELSE '3. Low   '
      END
    ) AS STRING) AS `Prediction Cat`,
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
  
  FROM Formula_93_0 AS in0

)

SELECT *

FROM Formula_93_1
