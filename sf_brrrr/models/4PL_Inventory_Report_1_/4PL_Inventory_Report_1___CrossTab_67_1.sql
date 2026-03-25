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
    SUM(AVAILABLEQUANTITY) AS SUM_AVAILABLEQUANTITY,
    ITEMNUMBER AS ITEMNUMBER,
    LOCATIONCODE AS LOCATIONCODE
  
  FROM LockInFilter_128 AS in0
  
  GROUP BY 
    ITEMNUMBER, LOCATIONCODE

),

CrossTab_67_0 AS (

  SELECT 
    (
      CASE
        WHEN (LOCATIONCODE IS NULL)
          THEN '_Null_'
        ELSE LOCATIONCODE
      END
    ) AS LOCATIONCODE,
    * EXCLUDE ("LOCATIONCODE")
  
  FROM LockInSummarize_58 AS in0

),

CrossTab_67_1 AS (

  SELECT 
    (REGEXP_REPLACE(LOCATIONCODE, '[\\s!@#$%^&*(),.?":{}|<>\\[\\]=;/\\-+]', '_')) AS LOCATIONCODE,
    * EXCLUDE ("LOCATIONCODE")
  
  FROM CrossTab_67_0 AS in0

)

SELECT *

FROM CrossTab_67_1
