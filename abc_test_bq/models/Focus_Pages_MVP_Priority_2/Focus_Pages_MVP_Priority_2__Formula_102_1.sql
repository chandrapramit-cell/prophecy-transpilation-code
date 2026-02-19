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

Formula_102_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (CAST(PregnancyDiag AS VARCHAR (1000)) IN ('Z3401', 'Z3481', 'Z3491'))
          OR (
               CAST(PregnancyDiag AS VARCHAR (1000)) IN (
                 'O99211',
                 'O99281',
                 'O99611',
                 'O99511',
                 'O99341',
                 'O99011',
                 'O99411',
                 'O98511',
                 'O99351',
                 'O9A211',
                 'O98811',
                 'O99331',
                 'O99111',
                 'O99321',
                 'O99841',
                 'O91111',
                 'O98311',
                 'O99711',
                 'O98111',
                 'O98211',
                 'O98911',
                 'O9A311',
                 'O99311',
                 'O91211',
                 'O26891',
                 'O26851',
                 'O2341',
                 'O24111',
                 'O2621',
                 'O26841',
                 'O2691',
                 'O24011',
                 'O2311',
                 'O23591',
                 'O26811',
                 'O2231',
                 'O26611',
                 'O24911',
                 'O2391',
                 'O2611',
                 'O24311',
                 'O2331',
                 'O23511',
                 'O2941',
                 'O2651',
                 'O26821',
                 'O29211',
                 'O26831',
                 'O2631'
               )
             )
        )
          THEN 'First Trimester'
        WHEN (
          (CAST(PregnancyDiag AS VARCHAR (1000)) IN ('Z3402', 'Z3482', 'Z3492'))
          OR (
               CAST(PregnancyDiag AS VARCHAR (1000)) IN (
                 'O99212',
                 'O99282',
                 'O99342',
                 'O99512',
                 'O99612',
                 'O99012',
                 'O99352',
                 'O98512',
                 'O9A212',
                 'O99412',
                 'O99112',
                 'O9A112',
                 'O99842',
                 'O99712',
                 'O99332',
                 'O99322',
                 'O98112',
                 'O98812',
                 'O98312',
                 'O9A312',
                 'O98412',
                 'O99312',
                 'O98912',
                 'O26892',
                 'O2342',
                 'O24112',
                 'O26872',
                 'O24012',
                 'O2622',
                 'O26852',
                 'O26832',
                 'O2312',
                 'O2302',
                 'O23592',
                 'O26812',
                 'O26612',
                 'O24912',
                 'O26842',
                 'O2692',
                 'O26712',
                 'O24312',
                 'O24812',
                 'O2612',
                 'O2242',
                 'O2392',
                 'O2332',
                 'O2232',
                 'O26642',
                 'O26822',
                 'O2602'
               )
             )
        )
          THEN 'Second Trimester'
        WHEN (
          (CAST(PregnancyDiag AS VARCHAR (1000)) IN ('Z3403', 'Z3483', 'Z3493'))
          OR (
               CAST(PregnancyDiag AS VARCHAR (1000)) IN (
                 'O99213',
                 'O99013',
                 'O99283',
                 'O98513',
                 'O99343',
                 'O99413',
                 'O99513',
                 'O99113',
                 'O99613',
                 'O9A213',
                 'O99353',
                 'O99713',
                 'O99843',
                 'O99333',
                 'O99323',
                 'O9A113',
                 'O98813',
                 'O98313',
                 'O98113',
                 'O91113',
                 'O98413',
                 'O99313',
                 'O98913',
                 'O26893',
                 'O24113',
                 'O24013',
                 'O26613',
                 'O26843',
                 'O2693',
                 'O2343',
                 'O26853',
                 'O2303',
                 'O26873',
                 'O2623',
                 'O24913',
                 'O23593',
                 'O26833',
                 'O2613',
                 'O2233',
                 'O26643',
                 'O24813',
                 'O2653',
                 'O26813',
                 'O2203',
                 'O2603',
                 'O2243',
                 'O2313',
                 'O2393',
                 'O2293',
                 'O24313',
                 'O228X3',
                 'O2333',
                 'O2943'
               )
             )
        )
          THEN 'Third Trimester'
        WHEN (
          (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS VARCHAR (1000)) IN ('Z34'))
          OR CAST(((STRPOS((coalesce(LOWER(LOWER(PregnancyDiagDesc)), '')), LOWER('unspecified trimester'))) > 0) AS BOOLEAN)
        )
          THEN 'Unspecified Trimester'
        ELSE 'Error'
      END
    ) AS VARCHAR (1000)) AS Trimester,
    *
  
  FROM Formula_90_0 AS in0

),

Formula_102_1 AS (

  SELECT 
    (
      CASE
        WHEN (Trimester = 'First Trimester')
          THEN 1
        WHEN (Trimester = 'Second Trimester')
          THEN 2
        WHEN (Trimester = 'Third Trimester')
          THEN 3
        ELSE 0
      END
    ) AS Trimester_INT,
    *
  
  FROM Formula_102_0 AS in0

)

SELECT *

FROM Formula_102_1
