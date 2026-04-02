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

Summarize_47 AS (

  SELECT 
    AVG("MEDIAN MONTHLY HOUSING COST  - RENTER") AS "AVG_MEDIAN MONTHLY HOUSING COST  - RENTER",
    AVG("MEDIAN MONTHLY HOUSING COST- OWNER") AS "AVG_MEDIAN MONTHLY HOUSING COST- OWNER",
    "INCOME CATEGORY" AS "INCOME CATEGORY"
  
  FROM Formula_29_1 AS in0
  
  GROUP BY "INCOME CATEGORY"

)

SELECT *

FROM Summarize_47
