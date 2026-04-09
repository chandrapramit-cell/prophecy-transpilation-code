{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_228 AS (

  SELECT *
  
  FROM {{ ref('p1__AlteryxSelect_228')}}

),

Formula_351_0 AS (

  SELECT 
    (
      CASE
        WHEN (ENDING_INVENTORY <= 0)
          THEN 'Projected Short Shipment for item from WH'
        ELSE NULL
      END
    ) AS ALERT_PROJECTED_SHORT_IN_WH_FOR_ITEM,
    *
  
  FROM AlteryxSelect_228 AS in0

),

Filter_354 AS (

  SELECT * 
  
  FROM Formula_351_0 AS in0
  
  WHERE (NOT(ALERT_PROJECTED_SHORT_IN_WH_FOR_ITEM IS NULL))

),

Summarize_355 AS (

  SELECT 
    DISTINCT SKU_STANDARD AS SKU_STANDARD,
    WH_ID_STANDARD AS WH_ID_STANDARD,
    ALERT_PROJECTED_SHORT_IN_WH_FOR_ITEM AS ALERT_PROJECTED_SHORT_IN_WH_FOR_ITEM
  
  FROM Filter_354 AS in0

)

SELECT *

FROM Summarize_355
