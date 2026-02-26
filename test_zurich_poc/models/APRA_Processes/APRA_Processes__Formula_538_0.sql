{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TextInput_98 AS (

  SELECT * 
  
  FROM {{ ref('seed_98')}}

),

TextInput_98_cast AS (

  SELECT CAST(pPeriod AS INTEGER) AS pPeriod
  
  FROM TextInput_98 AS in0

),

Formula_538_0 AS (

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
  
  FROM TextInput_98_cast AS in0

)

SELECT *

FROM Formula_538_0
