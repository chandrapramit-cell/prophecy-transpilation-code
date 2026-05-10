{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_372 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_372')}}

),

AlteryxSelect_387 AS (

  SELECT 
    "13_WK_WKLY_AVG_SALES" AS WKLY_AVG_SALES,
    * EXCLUDE ("13_WK_WKLY_AVG_SALES")
  
  FROM AlteryxSelect_372 AS in0

)

SELECT *

FROM AlteryxSelect_387
