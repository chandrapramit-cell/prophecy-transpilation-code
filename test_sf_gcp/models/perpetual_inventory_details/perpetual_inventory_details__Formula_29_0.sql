{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_27_0 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory_details__Formula_27_0')}}

),

Formula_29_0 AS (

  SELECT 
    (
      TO_CHAR(
        (
          CASE
            WHEN CAST((
              CAST((
                coalesce(
                  CAST(EXTRACT(MONTH FROM (TO_TIMESTAMP(ENDDATE))) AS DOUBLE), 
                  CAST(EXTRACT(MONTH FROM (TO_TIMESTAMP(ENDDATE))) AS INTEGER), 
                  0)
              ) AS DOUBLE) IN (2, 5, 8, 11)
            ) AS BOOLEAN)
              THEN (ADD_MONTHS(ENDDATE, -1))
            WHEN CAST((
              CAST((
                coalesce(
                  CAST(EXTRACT(MONTH FROM (TO_TIMESTAMP(ENDDATE))) AS DOUBLE), 
                  CAST(EXTRACT(MONTH FROM (TO_TIMESTAMP(ENDDATE))) AS INTEGER), 
                  0)
              ) AS DOUBLE) IN (3, 6, 9, 12)
            ) AS BOOLEAN)
              THEN (ADD_MONTHS(ENDDATE, -2))
            ELSE (ADD_MONTHS(ENDDATE, -3))
          END
        ), 
        'YYYY-MM-DD')
    ) AS PREVIOUSQUARTERCLOSINGDATE,
    *
  
  FROM Formula_27_0 AS in0

)

SELECT *

FROM Formula_29_0
