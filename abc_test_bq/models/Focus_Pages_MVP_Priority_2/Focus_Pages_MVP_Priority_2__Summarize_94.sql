{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_92 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Filter_92')}}

),

Summarize_94 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((PregnancyDiagDesc IS NULL) OR (CAST(PregnancyDiagDesc AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    PregnancyDiag AS PregnancyDiag,
    PregnancyDiagDesc AS PregnancyDiagDesc
  
  FROM Filter_92 AS in0
  
  GROUP BY 
    PregnancyDiag, PregnancyDiagDesc

)

SELECT *

FROM Summarize_94
