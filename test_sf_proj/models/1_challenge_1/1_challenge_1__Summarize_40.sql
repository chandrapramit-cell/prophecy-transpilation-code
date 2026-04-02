{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_29_1 AS (

  SELECT *
  
  FROM {{ ref('1_challenge_1__Formula_29_1')}}

),

Summarize_40 AS (

  SELECT 
    COUNT((
      CASE
        WHEN ((GEOID IS NULL) OR (GEOID = ''))
          THEN NULL
        ELSE 1
      END
    )) AS "COUNT",
    "INCOME CATEGORY" AS "INCOME CATEGORY"
  
  FROM Formula_29_1 AS in0
  
  GROUP BY "INCOME CATEGORY"

)

SELECT *

FROM Summarize_40
