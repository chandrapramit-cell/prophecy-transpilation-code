{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Cleanse_52 AS (

  SELECT *
  
  FROM {{ ref('32_2022_main__Cleanse_52')}}

),

Summarize_53 AS (

  SELECT 
    SUM(TotalUpperShots) AS Sum_TotalUpperShots,
    SUM(TotalLowerShots) AS Sum_TotalLowerShots,
    SUM(UpperShotAccuracy) AS Sum_UpperShotAccuracy,
    SUM(LowerShotAccuracy) AS Sum_LowerShotAccuracy,
    SUM(TotalShots) AS Sum_TotalShots,
    mode AS mode
  
  FROM Cleanse_52 AS in0
  
  GROUP BY mode

)

SELECT *

FROM Summarize_53
