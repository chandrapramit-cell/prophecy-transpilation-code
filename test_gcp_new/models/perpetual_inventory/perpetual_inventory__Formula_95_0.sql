{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Formula_2_0 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory__Formula_2_0')}}

),

Formula_95_0 AS (

  SELECT 
    (
      TO_DATE(
        (
          CASE
            WHEN CAST((
              CAST((
                coalesce(
                  CAST(EXTRACT(MONTH FROM EndDate) AS DOUBLE), 
                  CAST((REGEXP_EXTRACT(EXTRACT(MONTH FROM EndDate), '^[0-9]+', 0)) AS INTEGER), 
                  0)
              ) AS DOUBLE) IN (CAST(2 AS DOUBLE), CAST(5 AS DOUBLE), CAST(8 AS DOUBLE), CAST(11 AS DOUBLE))
            ) AS BOOLEAN)
              THEN (ADD_MONTHS(EndDate, -1))
            WHEN CAST((
              CAST((
                coalesce(
                  CAST(EXTRACT(MONTH FROM EndDate) AS DOUBLE), 
                  CAST((REGEXP_EXTRACT(EXTRACT(MONTH FROM EndDate), '^[0-9]+', 0)) AS INTEGER), 
                  0)
              ) AS DOUBLE) IN (CAST(3 AS DOUBLE), CAST(6 AS DOUBLE), CAST(9 AS DOUBLE), CAST(12 AS DOUBLE))
            ) AS BOOLEAN)
              THEN (ADD_MONTHS(EndDate, -2))
            ELSE (ADD_MONTHS(EndDate, -3))
          END
        ), 
        'yyyy-MM-dd')
    ) AS PreviousQuarterClosingDate,
    *
  
  FROM Formula_2_0 AS in0

)

SELECT *

FROM Formula_95_0
