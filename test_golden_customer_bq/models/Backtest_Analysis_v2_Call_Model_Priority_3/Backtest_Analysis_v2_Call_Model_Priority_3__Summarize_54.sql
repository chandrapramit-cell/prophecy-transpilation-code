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

Filter_55 AS (

  SELECT * 
  
  FROM AlteryxSelect_50 AS in0
  
  WHERE (YMD < '2023-02-01')

),

Summarize_53 AS (

  SELECT 
    MAX(`Called in 3`) AS `Max_Called in 3`,
    Household AS Household
  
  FROM Filter_55 AS in0
  
  GROUP BY Household

),

Summarize_54 AS (

  SELECT 
    COUNT(DISTINCT Household) AS CountDistinct_Household,
    `Max_Called in 3` AS `Max_Called in 3`
  
  FROM Summarize_53 AS in0
  
  GROUP BY `Max_Called in 3`

)

SELECT *

FROM Summarize_54
