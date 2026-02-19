{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_135_inner_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Join_135_inner_UnionLeftOuter')}}

),

Summarize_152 AS (

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
  
  FROM Join_135_inner_UnionLeftOuter AS in0
  
  GROUP BY Trimester

)

SELECT *

FROM Summarize_152
