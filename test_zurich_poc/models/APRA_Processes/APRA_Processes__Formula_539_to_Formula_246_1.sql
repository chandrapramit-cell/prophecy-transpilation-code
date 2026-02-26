{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TextInput_193 AS (

  SELECT * 
  
  FROM {{ ref('seed_193')}}

),

TextInput_193_cast AS (

  SELECT CAST(pPeriod AS INTEGER) AS pPeriod
  
  FROM TextInput_193 AS in0

),

Formula_539_to_Formula_246_0 AS (

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
  
  FROM TextInput_193_cast AS in0

),

Formula_539_to_Formula_246_1 AS (

  SELECT 
    (
      TO_DATE(
        (
          DATE_FORMAT(
            (
              DATE_ADD(
                (
                  ADD_MONTHS(
                    (
                      TO_TIMESTAMP(
                        (
                          REGEXP_REPLACE(
                            (
                              CONCAT(
                                (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(pPeriod AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
                                '01')
                            ), 
                            '\\.\\d+', 
                            '')
                        ), 
                        'yyyyMMdd')
                    ), 
                    1)
                ), 
                CAST(-1 AS INTEGER))
            ), 
            'yyyy-MM-dd')
        ), 
        'yyyy-MM-dd')
    ) AS pPeriodEnd,
    *
  
  FROM Formula_539_to_Formula_246_0 AS in0

)

SELECT *

FROM Formula_539_to_Formula_246_1
