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

Filter_60 AS (

  SELECT * 
  
  FROM AlteryxSelect_50 AS in0
  
  WHERE (`Blue Connect Household` = '1')

),

Summarize_61 AS (

  SELECT COUNT(DISTINCT Household) AS CountDistinct_Household
  
  FROM Filter_60 AS in0

)

SELECT *

FROM Summarize_61
