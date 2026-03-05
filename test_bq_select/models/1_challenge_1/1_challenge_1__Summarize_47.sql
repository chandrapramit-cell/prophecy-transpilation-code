{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_29_1 AS (

  SELECT *
  
  FROM {{ ref('1_challenge_1__Formula_29_1')}}

),

Summarize_47 AS (

  SELECT 
    AVG(`Median Monthly Housing Cost  - Renter`) AS `Avg_Median Monthly Housing Cost  - Renter`,
    AVG(`Median Monthly Housing Cost- Owner`) AS `Avg_Median Monthly Housing Cost- Owner`,
    `Income Category` AS `Income Category`
  
  FROM Formula_29_1 AS in0
  
  GROUP BY `Income Category`

)

SELECT *

FROM Summarize_47
