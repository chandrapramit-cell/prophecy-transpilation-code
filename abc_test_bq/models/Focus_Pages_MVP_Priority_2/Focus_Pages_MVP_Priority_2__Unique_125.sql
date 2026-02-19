{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH aka_alxaa2_Quer_116 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_116_ref') }}

),

Formula_118_0 AS (

  SELECT 
    CAST((CONCAT((SUBSTRING(`Service Date`, 1, 7)), '-01')) AS STRING) AS YEARMONTH,
    *
  
  FROM aka_alxaa2_Quer_116 AS in0

),

aka_alxaa2_Quer_119 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_119_ref') }}

),

Join_121_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`DIAG_CD_SK`)
  
  FROM Formula_118_0 AS in0
  INNER JOIN aka_alxaa2_Quer_119 AS in1
     ON (in0.DIAG_CD_1_SK = in1.DIAG_CD_SK)

),

AlteryxSelect_117 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(`Service Date` AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(`Service Date` AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(`Service Date` AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(`Service Date` AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(`Service Date` AS STRING)) AS DATE)
      END
    ) AS `Service Date`,
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(YEARMONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(YEARMONTH AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(YEARMONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(YEARMONTH AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(YEARMONTH AS STRING)) AS DATE)
      END
    ) AS YEARMONTH,
    * EXCEPT (`Service Date`, `YEARMONTH`)
  
  FROM Join_121_inner AS in0

),

Filter_123 AS (

  SELECT * 
  
  FROM AlteryxSelect_117 AS in0
  
  WHERE (
          (
            (
              (CAST((SUBSTRING(DIAG_CD, 1, 3)) AS VARCHAR (1000)) IN ('Z332', 'O03', 'O04'))
              OR (
                   CAST(PROC_CD AS VARCHAR (1000)) IN (
                     '59515',
                     '59514',
                     '59510',
                     '59400',
                     '59409',
                     '59410',
                     '59610',
                     '59612',
                     '59614',
                     '59409',
                     '59612',
                     '59618',
                     '59620',
                     '59622',
                     '59620',
                     '59409',
                     '59612',
                     'J7296',
                     'J7297',
                     'J7298',
                     'J7300',
                     'J7301',
                     'J1050'
                   )
                 )
            )
            OR CAST(((STRPOS((coalesce(LOWER(LOWER(PROC_CD_DESC)), '')), LOWER('salpingectomy'))) > 0) AS BOOLEAN)
          )
          OR CAST(((STRPOS((coalesce(LOWER(LOWER(PROC_CD_DESC)), '')), LOWER('oophorectomy'))) > 0) AS BOOLEAN)
        )

),

AlteryxSelect_124 AS (

  SELECT MBR_SK AS MBR_SK
  
  FROM Filter_123 AS in0

),

Unique_125 AS (

  SELECT * 
  
  FROM AlteryxSelect_124 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MBR_SK ORDER BY MBR_SK) = 1

)

SELECT *

FROM Unique_125
