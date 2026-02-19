{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_107_inner AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Join_107_inner')}}

),

Summarize_147 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((PregnancyDiagDesc IS NULL) OR (CAST(PregnancyDiagDesc AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    COUNT(DISTINCT MBR_ID) AS CountDistinct_MBR_ID,
    PregnancyDiagDesc AS PregnancyDiagDesc,
    Trimester AS Trimester
  
  FROM Join_107_inner AS in0
  
  GROUP BY 
    PregnancyDiagDesc, Trimester

),

Sort_149 AS (

  SELECT * 
  
  FROM Summarize_147 AS in0
  
  ORDER BY COUNT DESC

),

Filter_148 AS (

  SELECT * 
  
  FROM Sort_149 AS in0
  
  WHERE (Trimester = 'Third Trimester')

),

Sort_150 AS (

  SELECT * 
  
  FROM Filter_148 AS in0
  
  ORDER BY COUNT DESC

)

SELECT *

FROM Sort_150
