{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_90_0 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Formula_90_0')}}

),

Filter_114 AS (

  SELECT * 
  
  FROM Formula_90_0 AS in0
  
  WHERE (
          (
            (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'O03')
            OR (
                 (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'O04')
                 OR (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'P95')
               )
          )
          OR (
               (
                 (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'Z332')
                 OR (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'O021')
               )
               OR (
                    (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'O020')
                    OR (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'O028')
                  )
             )
        )

)

SELECT *

FROM Filter_114
