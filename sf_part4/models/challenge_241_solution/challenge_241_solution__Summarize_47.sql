{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH GenerateRows_44 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('challenge_241_solution', 'GenerateRows_44') }}

),

Formula_45_0 AS (

  SELECT 
    1 AS MONTHLY13TH,
    *
  
  FROM GenerateRows_44 AS in0

),

Formula_45_1 AS (

  SELECT 
    CAST((TO_CHAR(MONTHLY13TH, 'DY')) AS STRING) AS ACTUAL_DAY_OF_WEEK,
    *
  
  FROM Formula_45_0 AS in0

),

Filter_46 AS (

  SELECT * 
  
  FROM Formula_45_1 AS in0
  
  WHERE ('Fri' = ACTUAL_DAY_OF_WEEK)

),

Summarize_47 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN (("YEAR" IS NULL) OR (CAST("YEAR" AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS "COUNT",
    "YEAR" AS "YEAR"
  
  FROM Filter_46 AS in0
  
  GROUP BY "YEAR"

)

SELECT *

FROM Summarize_47
