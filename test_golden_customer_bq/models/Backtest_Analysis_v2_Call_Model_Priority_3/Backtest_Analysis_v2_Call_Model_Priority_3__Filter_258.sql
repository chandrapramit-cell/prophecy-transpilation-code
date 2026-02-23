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

Filter_258 AS (

  SELECT * 
  
  FROM Formula_93_1 AS in0
  
  WHERE (YMD = '2022-12-01')

)

SELECT *

FROM Filter_258
