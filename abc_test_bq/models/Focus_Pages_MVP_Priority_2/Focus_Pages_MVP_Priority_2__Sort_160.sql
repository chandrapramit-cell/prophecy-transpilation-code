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

Filter_114_reject_to_Filter_158 AS (

  SELECT * 
  
  FROM Formula_90_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  (
                    (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'O03')
                    OR (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'O04')
                  )
                  OR (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'P95'))
              )
              AND (
                    NOT(
                      (
                        (
                          (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'Z332')
                          OR (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'O021')
                        )
                        OR (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'O020')
                      )
                      OR (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'O028'))
                  )
            )
            OR (
                 (
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
                 ) IS NULL
               )
          )
          AND (
                (
                  (
                    (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'O03')
                    OR (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'O04')
                  )
                  OR (
                       (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'P95')
                       OR (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'Z332')
                     )
                )
                OR (
                     (
                       (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'O021')
                       OR (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'O020')
                     )
                     OR (
                          (CAST((SUBSTRING(PregnancyDiag, 1, 4)) AS STRING) = 'O028')
                          OR (CAST((SUBSTRING(PregnancyDiag, 1, 2)) AS STRING) = 'O2')
                        )
                   )
              )
        )

),

Summarize_159 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((MBR_ID IS NULL) OR (CAST(MBR_ID AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    COUNT(DISTINCT MBR_ID) AS CountDistinct_MBR_ID,
    PregnancyDiag AS PregnancyDiag,
    PregnancyDiagDesc AS PregnancyDiagDesc
  
  FROM Filter_114_reject_to_Filter_158 AS in0
  
  GROUP BY 
    PregnancyDiag, PregnancyDiagDesc

),

Sort_160 AS (

  SELECT * 
  
  FROM Summarize_159 AS in0
  
  ORDER BY COUNT DESC

)

SELECT *

FROM Sort_160
