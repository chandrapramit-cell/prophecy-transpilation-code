{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_50 AS (

  SELECT *
  
  FROM {{ ref('Backtest_Analysis_v2_Call_Model_Priority_3__AlteryxSelect_50')}}

),

Filter_2 AS (

  SELECT * 
  
  FROM AlteryxSelect_50 AS in0
  
  WHERE (PredictedLabel = '1')

)

SELECT *

FROM Filter_2
