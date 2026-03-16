{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH AlteryxSelect_139 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report__AlteryxSelect_139')}}

),

ItemNumberUploa_75 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('4PL_Inventory_Report', 'ItemNumberUploa_75') }}

),

AlteryxSelect_74 AS (

  SELECT CAST(ItemNumber AS string) AS ItemNumber
  
  FROM ItemNumberUploa_75 AS in0

),

Filter_76 AS (

  SELECT * 
  
  FROM AlteryxSelect_74 AS in0
  
  WHERE (
          (NOT(ItemNumber IS NULL))
          AND (
                (
                  NOT(
                    CAST(TRIM(ItemNumber) AS string) = '')
                ) OR (TRIM(ItemNumber) IS NULL)
              )
        )

),

Unique_84 AS (

  SELECT * 
  
  FROM Filter_76 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ItemNumber ORDER BY ItemNumber) = 1

),

LockInFilter_128 AS (

  SELECT *
  
  FROM {{ ref('4PL_Inventory_Report__LockInFilter_128')}}

),

LockInSummarize_3 AS (

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

Join_71_inner AS (

  SELECT 
    in0.ItemNumber AS ItemNumber,
    in0.ItemProductGroupCode AS ItemProductGroupCode,
    in0.ItemDescription AS ItemDescription,
    in0.Sum_AvailableQuantity AS Sum_AvailableQuantity,
    in1.ItemNumber AS Right_ItemNumber,
    in0.LocationCode AS LocationCode,
    in0.InventoryDate AS InventoryDate
  
  FROM LockInSummarize_3 AS in0
  INNER JOIN Unique_84 AS in1
     ON (in0.ItemNumber = in1.ItemNumber)

),

AppendFields_142 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM AlteryxSelect_139 AS in0
  INNER JOIN Join_71_inner AS in1
     ON TRUE

),

Formula_98_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (((`File Name` IS NULL) OR (`File Name` IS NULL)) OR ((LENGTH(`File Name`)) = 0))
          THEN CAST((CONCAT('Store_Inventory_ ', CURRENT_DATE)) AS string)
        ELSE `File Name`
      END
    ) AS string) AS `File Name`,
    * EXCEPT (`file name`)
  
  FROM AppendFields_142 AS in0

),

PortfolioComposerTable_6 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

ReportHeader_7 AS (

  {{ prophecy_basics.ToDo('Component type: ReportHeader is not supported.') }}

),

PortfolioComposerText_97 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

PortfolioComposerLayout_14 AS (

  {{ prophecy_basics.ToDo('Component type: Layout is not supported.') }}

),

PortfolioComposerRender_9 AS (

  {{ prophecy_basics.ToDo('Component type: Render is not supported.') }}

)

SELECT *

FROM PortfolioComposerRender_9
