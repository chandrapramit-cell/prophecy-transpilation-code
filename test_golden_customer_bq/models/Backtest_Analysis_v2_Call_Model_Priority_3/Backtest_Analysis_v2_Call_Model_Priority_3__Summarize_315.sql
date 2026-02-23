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

Summarize_315 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    `Blue Connect Household` AS `Blue Connect Household`,
    `Prediction Cat` AS `Prediction Cat`
  
  FROM Formula_93_1 AS in0
  
  GROUP BY 
    `Blue Connect Household`, `Prediction Cat`

)

SELECT *

FROM Summarize_315
