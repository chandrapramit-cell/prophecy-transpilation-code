{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH FICHERONTPA_xls_4 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Cuadre_Fidessa', 'FICHERONTPA_xls_4') }}

),

Formula_92_0 AS (

  SELECT 
    (
      CASE
        WHEN ("HUGO NBR" = '329890')
          THEN 'ES0177542018'
        WHEN ("HUGO NBR" = 'FN7514')
          THEN 'LU1598757687'
        WHEN ("HUGO NBR" = 'UF3441')
          THEN 'AU000000BKY0'
        ELSE "ISIN NBR"
      END
    ) AS "ISIN NBR",
    * EXCLUDE ("ISIN NBR")
  
  FROM FICHERONTPA_xls_4 AS in0

),

AlteryxSelect_45 AS (

  SELECT 
    "ISIN NBR" AS "ISIN NBR",
    "ORIGINAL QTY" AS "QTY NTPA",
    "ORIGINAL PRODS" AS "AMOUNT NTPA"
  
  FROM Formula_92_0 AS in0

),

Formula_46_0 AS (

  SELECT 
    CAST(("AMOUNT NTPA" / 100) AS FLOAT) AS "AMOUNT NTPA",
    * EXCLUDE ("AMOUNT NTPA")
  
  FROM AlteryxSelect_45 AS in0

)

SELECT *

FROM Formula_46_0
