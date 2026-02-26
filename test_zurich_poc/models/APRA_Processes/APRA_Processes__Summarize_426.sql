{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TextInput_5 AS (

  SELECT * 
  
  FROM {{ ref('seed_5')}}

),

TextInput_5_cast AS (

  SELECT CAST(pPeriod AS INTEGER) AS pPeriod
  
  FROM TextInput_5 AS in0

),

Formula_537_to_Formula_421_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (pPeriod <= 0)
          THEN (
            coalesce(
              CAST((DATE_FORMAT((DATE_ADD(CURRENT_TIMESTAMP, CAST(-27 AS INTEGER))), 'yyyyMM')) AS DOUBLE), 
              CAST((REGEXP_EXTRACT((DATE_FORMAT((DATE_ADD(CURRENT_TIMESTAMP, CAST(-27 AS INTEGER))), 'yyyyMM')), '^[0-9]+', 0)) AS INTEGER), 
              0)
          )
        ELSE pPeriod
      END
    ) AS INTEGER) AS pPeriod,
    * EXCEPT (`pperiod`)
  
  FROM TextInput_5_cast AS in0

),

Formula_537_to_Formula_421_1 AS (

  SELECT 
    CAST((
      coalesce(
        CAST((
          CONCAT(
            (
              SUBSTRING(
                CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(pPeriod AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')) AS STRING), 
                1, 
                4)
            ), 
            '01')
        ) AS DOUBLE), 
        CAST((
          REGEXP_EXTRACT(
            (
              CONCAT(
                (
                  SUBSTRING(
                    CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(pPeriod AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')) AS STRING), 
                    1, 
                    4)
                ), 
                '01')
            ), 
            '^[0-9]+', 
            0)
        ) AS INTEGER), 
        0)
    ) AS INTEGER) AS startPeriod,
    *
  
  FROM Formula_537_to_Formula_421_0 AS in0

),

GenerateRows_423 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Formula_537_to_Formula_421_1'], 
      '[{"name": "startPeriod", "dataType": "Integer"}, {"name": "pPeriod", "dataType": "Integer"}]', 
      'payload.startPeriod', 
      '(ytdPeriod <= payload.pPeriod)', 
      '(ytdPeriod + 1)', 
      'ytdPeriod', 
      '100', 
      'recursive'
    )
  }}

),

AlteryxSelect_425 AS (

  SELECT 
    CAST(ytdPeriod AS STRING) AS ytdPeriod,
    * EXCEPT (`ytdPeriod`)
  
  FROM GenerateRows_423 AS in0

),

Summarize_426 AS (

  SELECT 
    concat(concat('\'', concat_ws('\',\'', collect_list(ytdPeriod))), '\'') AS ytdPeriod,
    MAX(pPeriod) AS pPeriod
  
  FROM AlteryxSelect_425 AS in0

)

SELECT *

FROM Summarize_426
