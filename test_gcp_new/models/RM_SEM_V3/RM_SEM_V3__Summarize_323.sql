{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Join_311_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Join_311_left_UnionLeftOuter')}}

),

AlteryxSelect_306 AS (

  SELECT * EXCEPT (`RANK`, `AD_SCORE`)
  
  FROM Join_311_left_UnionLeftOuter AS in0

),

Transpose_316_schemaTransform_0 AS (

  SELECT * EXCEPT (`NEW_MARKET`, 
         `PLNG_REG_SHORT_NAME`, 
         `CARRIER_CNT`, 
         `SLACK_SEATS`, 
         `SHIP_CAP`, 
         `MILES`, 
         `Right_NDOD`, 
         `AS_QSI_PTS`, 
         `TTL_QSI_PTS`, 
         `AS_QSI`, 
         `PCT_SYS_ASMS`, 
         `SUPPORT_PROPOSED`)
  
  FROM AlteryxSelect_306 AS in0

),

Transpose_316 AS (

  SELECT 
    NDOD,
    Name,
    Value
  
  FROM Transpose_316_schemaTransform_0 AS in0
  UNPIVOT INCLUDE NULLS (
    Value
    FOR Name IN (
      FLOW_1, FLOW_2, FLOW_3
    )
  )

),

AlteryxSelect_322 AS (

  SELECT 
    Name AS FLOW,
    Value AS NDOD,
    * EXCEPT (`NDOD`, `Name`, `Value`)
  
  FROM Transpose_316 AS in0

),

Summarize_323 AS (

  SELECT DISTINCT NDOD AS NDOD
  
  FROM AlteryxSelect_322 AS in0

)

SELECT *

FROM Summarize_323
