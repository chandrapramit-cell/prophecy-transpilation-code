{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_119_0 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report_1___Formula_119_0')}}

),

RecordID_124 AS (

  {{
    prophecy_basics.RecordID(
      ['Formula_119_0'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Formula_125_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (RECORDID < 500000)
          THEN 'Sheet1'
        WHEN (RECORDID < 1000000)
          THEN 'Sheet2'
        WHEN (RECORDID < 1500000)
          THEN 'Sheet3'
        WHEN (RECORDID < 2000000)
          THEN 'Sheet4'
        WHEN (RECORDID < 2500000)
          THEN 'Sheet5'
        ELSE 'Sheet6'
      END
    ) AS STRING) AS SHEET,
    *
  
  FROM RecordID_124 AS in0

),

Formula_125_1 AS (

  SELECT 
    CAST(concat(`"FILE NAME"`, '.xlsx|||', SHEET) AS string) AS "FILE NAME",
    * EXCLUDE ("FILE NAME")
  
  FROM Formula_125_0 AS in0

),

AlteryxSelect_127 AS (

  SELECT 
    ITEMNUMBER AS ITEMNUMBER,
    ITEMDESCRIPTION AS ITEMDESCRIPTION,
    LOCATIONCODE AS LOCATIONCODE,
    SUM_AVAILABLEQUANTITY AS SUM_AVAILABLEQUANTITY,
    ITEMPRODUCTGROUPCODE AS ITEMPRODUCTGROUPCODE,
    INVENTORYDATE AS INVENTORYDATE,
    `"FILE NAME"` AS "FILE NAME"
  
  FROM Formula_125_1 AS in0

)

SELECT *

FROM AlteryxSelect_127
