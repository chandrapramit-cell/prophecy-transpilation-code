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

Filter_157_to_Filter_97 AS (

  SELECT * 
  
  FROM Formula_90_0 AS in0
  
  WHERE (
          (PregnancyDiag <> 'Z391')
          AND (
                (
                  (
                    CAST(PregnancyDiag AS VARCHAR (1000)) IN (
                      'Z3800',
                      'Z3801',
                      'Z3831',
                      'Z3830',
                      'Z382',
                      'Z3862',
                      'Z381',
                      'Z384',
                      'Z3861',
                      'Z385',
                      'Z3869',
                      'Z388',
                      'Z3863',
                      'O6014X0',
                      'O60',
                      'O6010X0',
                      'O6010X1',
                      'O6010X2',
                      'O6010X3',
                      'O6010X9',
                      'O6012',
                      'O6012X0',
                      'O6012X1',
                      'O6012X2',
                      'O6012X3',
                      'O6012X9',
                      'O6013X0',
                      'O6013X1',
                      'O6013X2',
                      'O6013X3',
                      'O6013X9',
                      'O6014',
                      'O6014X0',
                      'O6014X1',
                      'O6014X2',
                      'O6014X3',
                      'O6014X9',
                      'O602',
                      'O6020X0',
                      'O6020X1',
                      'O6022',
                      'O6022X0',
                      'O6022X1',
                      'O6023X0',
                      'O6023X1',
                      'O6023X2',
                      'O6023X3',
                      'O6023X9'
                    )
                  )
                  OR (CAST((SUBSTRING(PregnancyDiag, 1, 2)) AS VARCHAR (1000)) IN ('O8'))
                )
                OR (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS VARCHAR (1000)) IN ('Z37', 'Z39'))
              )
        )

)

SELECT *

FROM Filter_157_to_Filter_97
