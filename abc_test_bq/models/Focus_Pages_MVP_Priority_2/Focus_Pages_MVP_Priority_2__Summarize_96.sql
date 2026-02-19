{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_157_to_Filter_97 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Filter_157_to_Filter_97')}}

),

Summarize_96 AS (

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
  
  FROM Filter_157_to_Filter_97 AS in0
  
  GROUP BY PregnancyDiagDesc

)

SELECT *

FROM Summarize_96
