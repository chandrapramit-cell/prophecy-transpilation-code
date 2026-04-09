{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_347_1 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Formula_347_1')}}

),

Formula_350_0 AS (

  SELECT 
    (
      CASE
        WHEN (WEEKS_ON_HAND_INV <= 3)
          THEN 'Ending WOH for Item < or = 3 weeks'
        ELSE NULL
      END
    ) AS ALERT_ITEM_WOH_INV_LOW,
    (
      CASE
        WHEN (WEEKS_ON_HAND_INV >= 12)
          THEN 'Ending WOH for Item > or = 12 weeks'
        ELSE NULL
      END
    ) AS ALERT_ITEM_WOH_INV_HIGH,
    *
  
  FROM Formula_347_1 AS in0

)

SELECT *

FROM Formula_350_0
