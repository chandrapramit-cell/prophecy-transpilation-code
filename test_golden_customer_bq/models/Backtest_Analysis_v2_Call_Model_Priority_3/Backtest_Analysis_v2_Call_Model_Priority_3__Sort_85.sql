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

Filter_87 AS (

  SELECT * 
  
  FROM AlteryxSelect_50 AS in0
  
  WHERE (YMD = '2022-12-01')

),

Sort_85 AS (

  SELECT * 
  
  FROM Filter_87 AS in0
  
  ORDER BY Household ASC, YMD ASC

)

SELECT *

FROM Sort_85
