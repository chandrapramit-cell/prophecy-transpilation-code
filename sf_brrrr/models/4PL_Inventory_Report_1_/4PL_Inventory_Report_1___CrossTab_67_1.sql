{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH LockInFilter_128 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report_1___LockInFilter_128')}}

),

LockInSummarize_58 AS (

  SELECT 
    SUM(AvailableQuantity) AS Sum_AvailableQuantity,
    ItemNumber AS ItemNumber,
    LocationCode AS LocationCode
  
  FROM LockInFilter_128 AS in0
  
  GROUP BY 
    ItemNumber, LocationCode

),

CrossTab_67_0 AS (

  SELECT 
    (
      CASE
        WHEN (LocationCode IS NULL)
          THEN '_Null_'
        ELSE LocationCode
      END
    ) AS LocationCode,
    * EXCEPT (`locationcode`)
  
  FROM LockInSummarize_58 AS in0

),

CrossTab_67_1 AS (

  SELECT 
    (REGEXP_REPLACE(LocationCode, '[\\s!@#$%^&*(),.?":{}|<>\\[\\]=;/\\-+]', '_')) AS LocationCode,
    * EXCEPT (`locationcode`)
  
  FROM CrossTab_67_0 AS in0

)

SELECT *

FROM CrossTab_67_1
