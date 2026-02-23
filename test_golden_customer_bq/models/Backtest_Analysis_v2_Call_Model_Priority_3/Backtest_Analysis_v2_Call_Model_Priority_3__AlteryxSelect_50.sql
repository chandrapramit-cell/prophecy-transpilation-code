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

AlteryxSelect_50 AS (

  SELECT 
    CAST(Prediction AS FLOAT64) AS Prediction,
    * EXCEPT (`Prediction`)
  
  FROM Join_57_inner AS in0

)

SELECT *

FROM AlteryxSelect_50
