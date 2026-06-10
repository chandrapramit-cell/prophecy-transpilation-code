{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_311_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Join_311_left_UnionLeftOuter')}}

),

AlteryxSelect_317 AS (

  SELECT 
    SUPPORT_PROPOSED AS SUPPORT,
    * EXCEPT (`RANK`, 
    `PLNG_REG_SHORT_NAME`, 
    `AD_SCORE`, 
    `CARRIER_CNT`, 
    `SLACK_SEATS`, 
    `SHIP_CAP`, 
    `FLOW_1`, 
    `FLOW_2`, 
    `FLOW_3`, 
    `SUPPORT_PROPOSED`)
  
  FROM Join_311_left_UnionLeftOuter AS in0

),

Formula_321_0 AS (

  SELECT 
    CAST('DIRECT' AS string) AS variableTYPE,
    *
  
  FROM AlteryxSelect_317 AS in0

)

SELECT *

FROM Formula_321_0
