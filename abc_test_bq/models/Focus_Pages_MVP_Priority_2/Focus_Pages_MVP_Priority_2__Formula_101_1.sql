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

Formula_101_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (CAST(PregnancyDiag AS STRING) IN ('Z3A01', 'Z3A08', 'Z3A09', 'Z3A10', 'Z3A11', 'Z3A12'))
          THEN 'First Trimester      '
        WHEN (
          CAST(PregnancyDiag AS STRING) IN (
            'Z3A13',
            'Z3A14',
            'Z3A15',
            'Z3A16',
            'Z3A17',
            'Z3A18',
            'Z3A19',
            'Z3A20',
            'Z3A21',
            'Z3A22',
            'Z3A23',
            'Z3A24',
            'Z3A25',
            'Z3A26',
            'Z3A27'
          )
        )
          THEN 'Second Trimester     '
        WHEN (CAST(PregnancyDiag AS STRING) IN ('Z3A28', 'Z3A29', 'Z3A30', 'Z3A31', 'Z3A32', 'Z3A33', 'Z3A34', 'Z3A35', 'Z3A36', 'Z3A37', 'Z3A38', 'Z3A39', 'Z3A40'))
          THEN 'Third Trimester      '
        WHEN (CAST(PregnancyDiag AS STRING) IN ('Z3A41', 'Z3A42'))
          THEN '> 40 Weeks           '
        ELSE 'Unspecified Trimester'
      END
    ) AS STRING) AS Trimester,
    *
  
  FROM Filter_92 AS in0

),

Formula_101_1 AS (

  SELECT 
    (
      CASE
        WHEN (Trimester = 'First Trimester')
          THEN 1
        WHEN (Trimester = 'Second Trimester')
          THEN 2
        WHEN (Trimester = 'Third Trimester')
          THEN 3
        WHEN (Trimester = '> 40 Weeks')
          THEN 4
        ELSE 0
      END
    ) AS Trimester_INT,
    *
  
  FROM Formula_101_0 AS in0

)

SELECT *

FROM Formula_101_1
