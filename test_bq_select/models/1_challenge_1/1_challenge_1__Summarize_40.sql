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

Summarize_40 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((geoid IS NULL) OR (CAST(geoid AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    `Income Category` AS `Income Category`
  
  FROM Formula_29_1 AS in0
  
  GROUP BY `Income Category`

)

SELECT *

FROM Summarize_40
