{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_7_0 AS (

  SELECT *
  
  FROM {{ ref('boeing__Formula_7_0')}}

),

Summarize_66 AS (

  SELECT 
    SUM(ALTEA_SPILL) AS TTL_ALTEA_SPILL,
    SUM(SPILL_REV) AS TTL_SPILL_REV,
    SUM(EST_REV) AS TTL_EST_REV,
    SUM(BKGS) AS TTL_BKGS,
    SUM(POSIT_ALTEA_SPILL) AS TTL_POSIT_ALTEA_SPILL,
    SUM(ALTEA_DMD) AS TTL_ALTEA_DMD,
    CABIN AS CABIN,
    LEG_OD AS LEG_OD,
    BASE_DPTR_DATE AS BASE_DPTR_DATE
  
  FROM Formula_7_0 AS in0
  
  GROUP BY 
    CABIN, LEG_OD, BASE_DPTR_DATE

),

Formula_67_0 AS (

  SELECT 
    (TTL_EST_REV / TTL_BKGS) AS EST_FARE,
    *
  
  FROM Summarize_66 AS in0

),

Formula_67_1 AS (

  SELECT 
    (
      CASE
        WHEN ((TTL_SPILL_REV / TTL_ALTEA_SPILL) > (EST_FARE * 0.8))
          THEN (EST_FARE * 0.8)
        ELSE (TTL_SPILL_REV / TTL_ALTEA_SPILL)
      END
    ) AS SPILL_FARE,
    *
  
  FROM Formula_67_0 AS in0

),

AlteryxSelect_65 AS (

  SELECT *
  
  FROM Formula_67_1 AS in0

),

Filter_68 AS (

  SELECT * 
  
  FROM AlteryxSelect_65 AS in0
  
  WHERE (SPILL_FARE > CAST('0' AS FLOAT64))

)

SELECT *

FROM Filter_68
