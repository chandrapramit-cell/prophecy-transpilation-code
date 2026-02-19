{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_155 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Filter_155')}}

),

Summarize_156 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((Trimester IS NULL) OR (CAST(Trimester AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    COUNT(DISTINCT MBR_ID) AS CountDistinct_MBR_ID,
    Trimester AS Trimester
  
  FROM Filter_155 AS in0
  
  GROUP BY Trimester

)

SELECT *

FROM Summarize_156
