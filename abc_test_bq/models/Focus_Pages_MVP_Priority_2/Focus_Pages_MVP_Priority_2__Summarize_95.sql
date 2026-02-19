{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_93 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Filter_93')}}

),

Summarize_95 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((PregnancyDiagDesc IS NULL) OR (CAST(PregnancyDiagDesc AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    PregnancyDiagDesc AS PregnancyDiagDesc
  
  FROM Filter_93 AS in0
  
  GROUP BY PregnancyDiagDesc

)

SELECT *

FROM Summarize_95
