{{
  config({    
    "materialized": "table",
    "alias": "MajorCodes_w_Bu_126_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH AlteryxSelect_139 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report_Extended__AlteryxSelect_139')}}

),

LockInFilter_128 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report_Extended__LockInFilter_128')}}

),

LockInSummarize_103 AS (

  SELECT 
    SUM(AvailableQuantity) AS Sum_AvailableQuantity,
    LocationCode AS LocationCode,
    ItemProductGroupCode AS ItemProductGroupCode,
    ItemNumber AS ItemNumber,
    InventoryDate AS InventoryDate,
    ItemDescription AS ItemDescription
  
  FROM LockInFilter_128 AS in0
  
  GROUP BY 
    LocationCode, ItemProductGroupCode, ItemNumber, InventoryDate, ItemDescription

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
    CAST((
      CASE
        WHEN ((`File Name` IS NULL) OR ((LENGTH(`File Name`)) = 0))
          THEN CAST((CONCAT('Store_Inventory_ ', CAST(CURRENT_DATE AS STRING))) AS STRING)
        ELSE `File Name`
      END
    ) AS STRING) AS `File Name`,
    * EXCEPT (`file name`)
  
  FROM AppendFields_141 AS in0

),

Sort_116 AS (

  SELECT * 
  
  FROM Formula_119_0 AS in0
  
  ORDER BY ItemNumber ASC

),

RecordID_124 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM Sort_116

),

Formula_125_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (RecordID < 500000)
          THEN 'Sheet1'
        WHEN (RecordID < 1000000)
          THEN 'Sheet2'
        WHEN (RecordID < 1500000)
          THEN 'Sheet3'
        WHEN (RecordID < 2000000)
          THEN 'Sheet4'
        WHEN (RecordID < 2500000)
          THEN 'Sheet5'
        ELSE 'Sheet6'
      END
    ) AS STRING) AS Sheet,
    *
  
  FROM RecordID_124 AS in0

),

Formula_125_1 AS (

  SELECT 
    CAST((CONCAT(`File Name`, '.xlsx|||', Sheet)) AS STRING) AS `File Name`,
    * EXCEPT (`file name`)
  
  FROM Formula_125_0 AS in0

),

AlteryxSelect_127 AS (

  SELECT 
    ItemNumber AS ItemNumber,
    ItemDescription AS ItemDescription,
    LocationCode AS LocationCode,
    Sum_AvailableQuantity AS Sum_AvailableQuantity,
    ItemProductGroupCode AS ItemProductGroupCode,
    InventoryDate AS InventoryDate,
    `File Name` AS `File Name`
  
  FROM Formula_125_1 AS in0

)

SELECT *

FROM AlteryxSelect_127
