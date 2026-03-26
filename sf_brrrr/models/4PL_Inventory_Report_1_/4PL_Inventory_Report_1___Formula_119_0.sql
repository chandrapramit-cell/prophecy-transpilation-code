{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_139 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report_1___AlteryxSelect_139')}}

),

LockInFilter_128 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report_1___LockInFilter_128')}}

),

LockInSummarize_103 AS (

  SELECT 
    SUM(AVAILABLEQUANTITY) AS SUM_AVAILABLEQUANTITY,
    LOCATIONCODE AS LOCATIONCODE,
    ITEMPRODUCTGROUPCODE AS ITEMPRODUCTGROUPCODE,
    ITEMNUMBER AS ITEMNUMBER,
    INVENTORYDATE AS INVENTORYDATE,
    ITEMDESCRIPTION AS ITEMDESCRIPTION
  
  FROM LockInFilter_128 AS in0
  
  GROUP BY 
    LOCATIONCODE, ITEMPRODUCTGROUPCODE, ITEMNUMBER, INVENTORYDATE, ITEMDESCRIPTION

),

AppendFields_141 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM AlteryxSelect_139 AS in0
  INNER JOIN LockInSummarize_103 AS in1
     ON TRUE

),

Formula_119_0 AS (

  SELECT 
    CAST(CASE
      WHEN "FILE NAME" IS NULL or "FILE NAME" IS NULL or (length("FILE NAME") = 0)
        THEN concat('Store_Inventory_ ', current_date())
      ELSE "FILE NAME"
    END AS string) AS "FILE NAME",
    * EXCLUDE ("FILE NAME")
  
  FROM AppendFields_141 AS in0

)

SELECT *

FROM Formula_119_0
