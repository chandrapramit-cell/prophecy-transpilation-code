{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Cleanse_214 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Cleanse_214')}}

),

Unique_321_window AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY SOURCE_SKU ORDER BY SOURCE_SKU ASC NULLS FIRST) AS ROW_NUMBER
  
  FROM Cleanse_214 AS in0

),

Unique_321_filter AS (

  SELECT * 
  
  FROM Unique_321_window AS in0
  
  WHERE (ROW_NUMBER > 1)

),

Unique_321_drop_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM Unique_321_filter AS in0

),

Formula_394_0 AS (

  SELECT 
    CAST('Duplicate SOURCE_SKU in Item Master File' AS STRING) AS DQ_ISSUE,
    CAST('Remove the duplicate SOURCE_SKU in the Item Master File' AS STRING) AS ACTION,
    *
  
  FROM Unique_321_drop_0 AS in0

),

Union_395_reformat_5 AS (

  SELECT 
    ACTION AS ACTION,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(SKU_CATEGORY AS STRING) AS SKU_CATEGORY,
    CAST(SKU_DESC_STANDARD AS STRING) AS SKU_DESC_STANDARD,
    CAST(SKU_STANDARD AS STRING) AS SKU_STANDARD,
    CAST(SOURCE_SKU AS STRING) AS SOURCE_SKU,
    CAST(SOURCE_SKU_DESC AS STRING) AS SOURCE_SKU_DESC
  
  FROM Formula_394_0 AS in0

),

Formula_282_0 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_282_0')}}

),

AlteryxSelect_338 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_338')}}

),

Formula_237_0 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_237_0')}}

),

Join_339_right AS (

  SELECT in0.*
  
  FROM AlteryxSelect_338 AS in0
  LEFT JOIN Formula_237_0 AS in1
     ON (in1.SKU_STANDARD = in0.SKU_STANDARD)

),

Cleanse_316 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Cleanse_316')}}

),

Unique_397_window AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY SOURCEWHDESCRIPTION ORDER BY SOURCEWHDESCRIPTION ASC NULLS FIRST) AS ROW_NUMBER
  
  FROM Cleanse_316 AS in0

),

Unique_397_filter AS (

  SELECT * 
  
  FROM Unique_397_window AS in0
  
  WHERE (ROW_NUMBER > 1)

),

Unique_397_drop_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM Unique_397_filter AS in0

),

Formula_399_0 AS (

  SELECT 
    CAST('"DUPLICATE SOURCE_WH_DESC IN WH MASTER FILE"' AS STRING) AS DQ_ISSUE,
    CAST('"REMOVE THE DUPLICATE SOURCE_WH_DESC IN THE WH MASTER FILE"' AS STRING) AS ACTION,
    *
  
  FROM Unique_397_drop_0 AS in0

),

Summarize_355 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Summarize_355')}}

),

Formula_433_0 AS (

  SELECT 
    CAST('Potential for short shipment - One or more upcoming PO\'s may cause the available inventory to go negative, resulting in a short shipment.' AS STRING) AS DQ_ISSUE,
    CAST('Produce more product, transfer more product into this warehouse, or make arrangements to ship this order from another warehouse with stock.' AS STRING) AS ACTION,
    *
  
  FROM Summarize_355 AS in0

),

Union_395_reformat_10 AS (

  SELECT 
    ACTION AS ACTION,
    CAST(ALERT_PROJECTED_SHORT_IN_WH_FOR_ITEM AS STRING) AS ALERT_PROJECTED_SHORT_IN_WH_FOR_ITEM,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(SKU_STANDARD AS STRING) AS SKU_STANDARD,
    CAST(WH_ID_STANDARD AS STRING) AS WH_ID_STANDARD
  
  FROM Formula_433_0 AS in0

),

Filter_480 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Filter_480')}}

),

Formula_215_0 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_215_0')}}

),

Join_332_left AS (

  SELECT in0.*
  
  FROM Filter_480 AS in0
  LEFT JOIN Formula_215_0 AS in1
     ON (in0.SKU = in1.SOURCE_SKU)

),

Formula_401_0 AS (

  SELECT 
    CAST('SKU in WOH Input File does not have a matching SOURCE_SKU in Item Master' AS STRING) AS DQ_ISSUE,
    CAST('Update the SKU in either WOH Input File or Item Master to match.' AS STRING) AS ACTION,
    *
  
  FROM Join_332_left AS in0

),

AlteryxSelect_400 AS (

  SELECT 
    SKU AS SOURCE_SKU,
    DQ_ISSUE AS DQ_ISSUE,
    ACTION AS ACTION
  
  FROM Formula_401_0 AS in0

),

Union_449 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Union_449')}}

),

Unique_587_window AS (

  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY WH_ID_STANDARD, 
    WH_DESC_STANDARD, 
    SOURCE_WH_DESC, 
    FILENAME, 
    DATE_OF_EVENT, 
    ROWTYPE, 
    ROWSORTTIER, 
    CUSTOMER, 
    SALES_ORDER, 
    PO_NUMBER, 
    PICKUP_OR_DELIVERY, 
    SHIP_DT, 
    DELIVERY_DT, 
    PRODUCTION_OR_TRANSFER, 
    SKU_STANDARD, 
    SKU_DESC_STANDARD, 
    SKU_CATEGORY ORDER BY WH_ID_STANDARD ASC NULLS FIRST, WH_DESC_STANDARD ASC NULLS FIRST, SOURCE_WH_DESC ASC NULLS FIRST, FILENAME ASC NULLS FIRST, DATE_OF_EVENT ASC NULLS FIRST, ROWTYPE ASC NULLS FIRST, ROWSORTTIER ASC NULLS FIRST, CUSTOMER ASC NULLS FIRST, SALES_ORDER ASC NULLS FIRST, PO_NUMBER ASC NULLS FIRST, PICKUP_OR_DELIVERY ASC NULLS FIRST, SHIP_DT ASC NULLS FIRST, DELIVERY_DT ASC NULLS FIRST, PRODUCTION_OR_TRANSFER ASC NULLS FIRST, SKU_STANDARD ASC NULLS FIRST, SKU_DESC_STANDARD ASC NULLS FIRST, SKU_CATEGORY ASC NULLS FIRST) AS ROW_NUMBER
  
  FROM Union_449 AS in0

),

Unique_587_filter AS (

  SELECT * 
  
  FROM Unique_587_window AS in0
  
  WHERE (ROW_NUMBER > 1)

),

Unique_587_drop_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM Unique_587_filter AS in0

),

Unique_587 AS (

  SELECT * 
  
  FROM Union_449 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY WH_ID_STANDARD, 
  WH_DESC_STANDARD, 
  SOURCE_WH_DESC, 
  FILENAME, 
  DATE_OF_EVENT, 
  ROWTYPE, 
  ROWSORTTIER, 
  CUSTOMER, 
  SALES_ORDER, 
  PO_NUMBER, 
  PICKUP_OR_DELIVERY, 
  SHIP_DT, 
  DELIVERY_DT, 
  PRODUCTION_OR_TRANSFER, 
  SKU_STANDARD, 
  SKU_DESC_STANDARD, 
  SKU_CATEGORY ORDER BY WH_ID_STANDARD, WH_DESC_STANDARD, SOURCE_WH_DESC, FILENAME, DATE_OF_EVENT, ROWTYPE, ROWSORTTIER, CUSTOMER, SALES_ORDER, PO_NUMBER, PICKUP_OR_DELIVERY, SHIP_DT, DELIVERY_DT, PRODUCTION_OR_TRANSFER, SKU_STANDARD, SKU_DESC_STANDARD, SKU_CATEGORY) = 1

),

Join_592_inner AS (

  SELECT 
    in1.WH_ID_STANDARD AS RIGHT_WH_ID_STANDARD,
    in1.WH_DESC_STANDARD AS RIGHT_WH_DESC_STANDARD,
    in1.SOURCE_WH_DESC AS RIGHT_SOURCE_WH_DESC,
    in1.SOURCE_SKU AS RIGHT_SOURCE_SKU,
    in1.QTY AS RIGHT_QTY,
    in1.FILENAME AS RIGHT_FILENAME,
    in1.DATE_OF_EVENT AS RIGHT_DATE_OF_EVENT,
    in1.ROWTYPE AS RIGHT_ROWTYPE,
    in1.ROWSORTTIER AS RIGHT_ROWSORTTIER,
    in1.CUSTOMER AS RIGHT_CUSTOMER,
    in1.SALES_ORDER AS RIGHT_SALES_ORDER,
    in1.PO_NUMBER AS RIGHT_PO_NUMBER,
    in1.PICKUP_OR_DELIVERY AS RIGHT_PICKUP_OR_DELIVERY,
    in1.SHIP_DT AS RIGHT_SHIP_DT,
    in1.DELIVERY_DT AS RIGHT_DELIVERY_DT,
    in1.PRODUCTION_OR_TRANSFER AS RIGHT_PRODUCTION_OR_TRANSFER,
    in1.SOURCE_SKU_DESC AS RIGHT_SOURCE_SKU_DESC,
    in1.SKU_STANDARD AS RIGHT_SKU_STANDARD,
    in1.SKU_DESC_STANDARD AS RIGHT_SKU_DESC_STANDARD,
    in1.SKU_CATEGORY AS RIGHT_SKU_CATEGORY,
    in0.*,
    in1.* EXCLUDE ("WH_ID_STANDARD", 
    "WH_DESC_STANDARD", 
    "SOURCE_WH_DESC", 
    "SOURCE_SKU", 
    "QTY", 
    "FILENAME", 
    "DATE_OF_EVENT", 
    "ROWTYPE", 
    "ROWSORTTIER", 
    "CUSTOMER", 
    "SALES_ORDER", 
    "PO_NUMBER", 
    "PICKUP_OR_DELIVERY", 
    "SHIP_DT", 
    "DELIVERY_DT", 
    "PRODUCTION_OR_TRANSFER", 
    "SOURCE_SKU_DESC", 
    "SKU_STANDARD", 
    "SKU_DESC_STANDARD", 
    "SKU_CATEGORY")
  
  FROM Unique_587 AS in0
  INNER JOIN Unique_587_drop_0 AS in1
     ON (
      (
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          ((in0.WH_ID_STANDARD = in1.WH_ID_STANDARD) AND (in0.SKU_STANDARD = in1.SKU_STANDARD))
                          AND (in0.ROWTYPE = in1.ROWTYPE)
                        )
                        AND (in0.DATE_OF_EVENT = in1.DATE_OF_EVENT)
                      )
                      AND (in0.ROWSORTTIER = in1.ROWSORTTIER)
                    )
                    AND (in0.CUSTOMER = in1.CUSTOMER)
                  )
                  AND (in0.SALES_ORDER = in1.SALES_ORDER)
                )
                AND (in0.PO_NUMBER = in1.PO_NUMBER)
              )
              AND (in0.PICKUP_OR_DELIVERY = in1.PICKUP_OR_DELIVERY)
            )
            AND (in0.SHIP_DT = in1.SHIP_DT)
          )
          AND (in0.DELIVERY_DT = in1.DELIVERY_DT)
        )
        AND (in0.PRODUCTION_OR_TRANSFER = in1.PRODUCTION_OR_TRANSFER)
      )
      AND (in0.SKU_STANDARD = in1.SKU_STANDARD)
    )

),

AlteryxSelect_596 AS (

  SELECT 
    WH_ID_STANDARD AS WH_ID_STANDARD,
    WH_DESC_STANDARD AS WH_DESC_STANDARD,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SOURCE_SKU AS SOURCE_SKU,
    QTY AS QTY,
    FILENAME AS FILENAME,
    DATE_OF_EVENT AS DATE_OF_EVENT,
    ROWTYPE AS ROWTYPE,
    ROWSORTTIER AS ROWSORTTIER,
    CUSTOMER AS CUSTOMER,
    SALES_ORDER AS SALES_ORDER,
    PO_NUMBER AS PO_NUMBER,
    PICKUP_OR_DELIVERY AS PICKUP_OR_DELIVERY,
    SHIP_DT AS SHIP_DT,
    DELIVERY_DT AS DELIVERY_DT,
    PRODUCTION_OR_TRANSFER AS PRODUCTION_OR_TRANSFER,
    SOURCE_SKU_DESC AS SOURCE_SKU_DESC,
    SKU_STANDARD AS SKU_STANDARD,
    SKU_DESC_STANDARD AS SKU_DESC_STANDARD,
    SKU_CATEGORY AS SKU_CATEGORY
  
  FROM Join_592_inner AS in0

),

Union_599_reformat_0 AS (

  SELECT 
    CUSTOMER AS CUSTOMER,
    (TO_CHAR(TO_DATE(DATE_OF_EVENT), 'YYYY-MM-DD')) AS DATE_OF_EVENT,
    (TO_CHAR(TO_DATE(DELIVERY_DT), 'YYYY-MM-DD')) AS DELIVERY_DT,
    FILENAME AS FILENAME,
    PICKUP_OR_DELIVERY AS PICKUP_OR_DELIVERY,
    PO_NUMBER AS PO_NUMBER,
    PRODUCTION_OR_TRANSFER AS PRODUCTION_OR_TRANSFER,
    CAST(QTY AS STRING) AS QTY,
    CAST(ROWSORTTIER AS STRING) AS ROWSORTTIER,
    ROWTYPE AS ROWTYPE,
    SALES_ORDER AS SALES_ORDER,
    (TO_CHAR(TO_DATE(SHIP_DT), 'YYYY-MM-DD')) AS SHIP_DT,
    SKU_CATEGORY AS SKU_CATEGORY,
    SKU_DESC_STANDARD AS SKU_DESC_STANDARD,
    SKU_STANDARD AS SKU_STANDARD,
    SOURCE_SKU AS SOURCE_SKU,
    SOURCE_SKU_DESC AS SOURCE_SKU_DESC,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    WH_DESC_STANDARD AS WH_DESC_STANDARD,
    WH_ID_STANDARD AS WH_ID_STANDARD
  
  FROM AlteryxSelect_596 AS in0

),

AlteryxSelect_597 AS (

  SELECT 
    RIGHT_WH_ID_STANDARD AS RIGHT_WH_ID_STANDARD,
    RIGHT_WH_DESC_STANDARD AS RIGHT_WH_DESC_STANDARD,
    RIGHT_SOURCE_WH_DESC AS RIGHT_SOURCE_WH_DESC,
    RIGHT_SOURCE_SKU AS RIGHT_SOURCE_SKU,
    RIGHT_QTY AS RIGHT_QTY,
    RIGHT_FILENAME AS RIGHT_FILENAME,
    RIGHT_DATE_OF_EVENT AS RIGHT_DATE_OF_EVENT,
    RIGHT_ROWTYPE AS RIGHT_ROWTYPE,
    RIGHT_ROWSORTTIER AS RIGHT_ROWSORTTIER,
    RIGHT_CUSTOMER AS RIGHT_CUSTOMER,
    RIGHT_SALES_ORDER AS RIGHT_SALES_ORDER,
    RIGHT_PO_NUMBER AS RIGHT_PO_NUMBER,
    RIGHT_PICKUP_OR_DELIVERY AS RIGHT_PICKUP_OR_DELIVERY,
    RIGHT_SHIP_DT AS RIGHT_SHIP_DT,
    RIGHT_DELIVERY_DT AS RIGHT_DELIVERY_DT,
    RIGHT_PRODUCTION_OR_TRANSFER AS RIGHT_PRODUCTION_OR_TRANSFER,
    RIGHT_SOURCE_SKU_DESC AS RIGHT_SOURCE_SKU_DESC,
    RIGHT_SKU_STANDARD AS RIGHT_SKU_STANDARD,
    RIGHT_SKU_DESC_STANDARD AS RIGHT_SKU_DESC_STANDARD,
    RIGHT_SKU_CATEGORY AS RIGHT_SKU_CATEGORY
  
  FROM Join_592_inner AS in0

),

DynamicRename_598 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_597'], 
      [
        'RIGHT_WH_ID_STANDARD', 
        'RIGHT_CUSTOMER', 
        'RIGHT_SOURCE_WH_DESC', 
        'RIGHT_SOURCE_SKU', 
        'RIGHT_SALES_ORDER', 
        'RIGHT_ROWSORTTIER', 
        'RIGHT_ROWTYPE', 
        'RIGHT_SHIP_DT', 
        'RIGHT_DELIVERY_DT', 
        'RIGHT_PICKUP_OR_DELIVERY', 
        'RIGHT_QTY', 
        'RIGHT_SKU_DESC_STANDARD', 
        'RIGHT_WH_DESC_STANDARD', 
        'RIGHT_SKU_CATEGORY', 
        'RIGHT_SOURCE_SKU_DESC', 
        'RIGHT_DATE_OF_EVENT', 
        'RIGHT_PO_NUMBER', 
        'RIGHT_SKU_STANDARD', 
        'RIGHT_FILENAME', 
        'RIGHT_PRODUCTION_OR_TRANSFER'
      ], 
      'advancedRename', 
      [
        'RIGHT_WH_ID_STANDARD', 
        'RIGHT_WH_DESC_STANDARD', 
        'RIGHT_SOURCE_WH_DESC', 
        'RIGHT_SOURCE_SKU', 
        'RIGHT_QTY', 
        'RIGHT_FILENAME', 
        'RIGHT_DATE_OF_EVENT', 
        'RIGHT_ROWTYPE', 
        'RIGHT_ROWSORTTIER', 
        'RIGHT_CUSTOMER', 
        'RIGHT_SALES_ORDER', 
        'RIGHT_PO_NUMBER', 
        'RIGHT_PICKUP_OR_DELIVERY', 
        'RIGHT_SHIP_DT', 
        'RIGHT_DELIVERY_DT', 
        'RIGHT_PRODUCTION_OR_TRANSFER', 
        'RIGHT_SOURCE_SKU_DESC', 
        'RIGHT_SKU_STANDARD', 
        'RIGHT_SKU_DESC_STANDARD', 
        'RIGHT_SKU_CATEGORY'
      ], 
      'Suffix', 
      '', 
      "(REGEXP_REPLACE(column_name, 'Right_', ''))"
    )
  }}

),

Union_599_reformat_1 AS (

  SELECT 
    CUSTOMER AS CUSTOMER,
    1 AS DATE_OF_EVENT,
    1 AS DELIVERY_DT,
    FILENAME AS FILENAME,
    PICKUP_OR_DELIVERY AS PICKUP_OR_DELIVERY,
    PO_NUMBER AS PO_NUMBER,
    PRODUCTION_OR_TRANSFER AS PRODUCTION_OR_TRANSFER,
    CAST(QTY AS STRING) AS QTY,
    CAST(ROWSORTTIER AS STRING) AS ROWSORTTIER,
    ROWTYPE AS ROWTYPE,
    SALES_ORDER AS SALES_ORDER,
    1 AS SHIP_DT,
    SKU_CATEGORY AS SKU_CATEGORY,
    SKU_DESC_STANDARD AS SKU_DESC_STANDARD,
    SKU_STANDARD AS SKU_STANDARD,
    SOURCE_SKU AS SOURCE_SKU,
    SOURCE_SKU_DESC AS SOURCE_SKU_DESC,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    WH_DESC_STANDARD AS WH_DESC_STANDARD,
    WH_ID_STANDARD AS WH_ID_STANDARD
  
  FROM DynamicRename_598 AS in0

),

Union_599 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_599_reformat_0', 'Union_599_reformat_1'], 
      [
        '[{"name": "CUSTOMER", "dataType": "String"}, {"name": "DATE_OF_EVENT", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "QTY", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "String"}, {"name": "SKU_CATEGORY", "dataType": "String"}, {"name": "SKU_DESC_STANDARD", "dataType": "String"}, {"name": "SKU_STANDARD", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}, {"name": "SOURCE_SKU_DESC", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "WH_DESC_STANDARD", "dataType": "String"}, {"name": "WH_ID_STANDARD", "dataType": "String"}]', 
        '[{"name": "CUSTOMER", "dataType": "String"}, {"name": "DATE_OF_EVENT", "dataType": "Number"}, {"name": "DELIVERY_DT", "dataType": "Number"}, {"name": "FILENAME", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "QTY", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "Number"}, {"name": "SKU_CATEGORY", "dataType": "String"}, {"name": "SKU_DESC_STANDARD", "dataType": "String"}, {"name": "SKU_STANDARD", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}, {"name": "SOURCE_SKU_DESC", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "WH_DESC_STANDARD", "dataType": "String"}, {"name": "WH_ID_STANDARD", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_604_0 AS (

  SELECT 
    CAST('Duplicate entries in source systems for same standard SKU' AS STRING) AS DQ_ISSUE,
    CAST('Check source file to see if items should be consolidated in source system OR split into two items in Item Master.' AS STRING) AS ACTION,
    *
  
  FROM Union_599 AS in0

),

Formula_350_0 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_350_0')}}

),

Transpose_424 AS (

  {{
    prophecy_basics.Transpose(
      ['Formula_350_0'], 
      ['SKU_STANDARD'], 
      [
        'ALERT_ITEM_WOH_INV_LOW', 
        'ALERT_ITEM_WOH_INV_HIGH', 
        'PRODUCTION_QTY', 
        'BEGINNING_INVENTORY', 
        'ROWSORTTIER', 
        'SALES_QTY', 
        'DATE_OF_EVENT', 
        'WEEKS_ON_HAND_INV', 
        '13_WK_WKLY_AVG_SALES', 
        'ENDING_INVENTORY', 
        'TRANSFER_QTY', 
        'ROWTYPE', 
        'WH_ID_STANDARD'
      ], 
      'NAME', 
      'VALUE', 
      [
        'ALERT_ITEM_WOH_INV_LOW', 
        'ALERT_ITEM_WOH_INV_HIGH', 
        'WEEKS_ON_HAND_INV', 
        '13_WK_WKLY_AVG_SALES', 
        'ROWTYPE', 
        'ROWSORTTIER', 
        'WH_ID_STANDARD', 
        'DATE_OF_EVENT', 
        'BEGINNING_INVENTORY', 
        'SALES_QTY', 
        'PRODUCTION_QTY', 
        'TRANSFER_QTY', 
        'ENDING_INVENTORY', 
        'SKU_STANDARD'
      ], 
      true
    )
  }}

),

Filter_426 AS (

  SELECT * 
  
  FROM Transpose_424 AS in0
  
  WHERE ("VALUE" IS NOT NULL)

),

Formula_429_0 AS (

  SELECT 
    CAST((CONCAT("NAME", ' ', "VALUE")) AS STRING) AS DQ_ISSUE,
    CAST(CASE
      WHEN CAST(coalesce(contains(lower(NAME), lower('LOW')), false) AS BOOLEAN)
        THEN 'Consider producing more of this item so you don\'t run out, ship short, and lose revenue/upset buyers.'
      ELSE 'Find a way to move more product for this item so it doesn\'t sit around taking up space and capital or worse, expire.'
    END AS STRING) AS ACTION,
    *
  
  FROM Filter_426 AS in0

),

AlteryxSelect_547 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_547')}}

),

Join_540_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_547 AS in0
  LEFT JOIN Formula_215_0 AS in1
     ON (in0.SKU = in1.SOURCE_SKU)

),

AlteryxSelect_398 AS (

  SELECT 
    SOURCEWHDESCRIPTION AS SOURCE_WH_DESC,
    DQ_ISSUE AS DQ_ISSUE,
    ACTION AS ACTION
  
  FROM Formula_399_0 AS in0

),

Union_395_reformat_0 AS (

  SELECT 
    ACTION AS ACTION,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(SOURCE_WH_DESC AS STRING) AS SOURCE_WH_DESC
  
  FROM AlteryxSelect_398 AS in0

),

Filter_439 AS (

  SELECT * 
  
  FROM Formula_282_0 AS in0
  
  WHERE (SOURCE_WH_DESC = '5000')

),

Formula_441_0 AS (

  SELECT 
    CAST('Unknown Ship-From WH for PO - The PO in Quickbooks does not indicate which warehouse this order will ship from.' AS STRING) AS DQ_ISSUE,
    CAST('Go look at the PO in Quickbooks and assign a warehouse and tag it for pickup or delivery using the appropriate comment field in QBO.' AS STRING) AS ACTION,
    *
  
  FROM Filter_439 AS in0

),

Union_395_reformat_11 AS (

  SELECT 
    ACTION AS ACTION,
    CAST(CUSTOMER AS STRING) AS CUSTOMER,
    1 AS DATE_OF_EVENT,
    1 AS DELIVERY_DT,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(FILENAME AS STRING) AS FILENAME,
    CAST(PICKUP_OR_DELIVERY AS STRING) AS PICKUP_OR_DELIVERY,
    CAST(PO_NUMBER AS STRING) AS PO_NUMBER,
    CAST(QTY AS STRING) AS QTY,
    CAST(ROWSORTTIER AS STRING) AS ROWSORTTIER,
    CAST(ROWTYPE AS STRING) AS ROWTYPE,
    CAST(SALES_ORDER AS STRING) AS SALES_ORDER,
    1 AS SHIP_DT,
    CAST(SKU AS STRING) AS SKU,
    CAST(SOURCE_WH_DESC AS STRING) AS SOURCE_WH_DESC
  
  FROM Formula_441_0 AS in0

),

Join_202_left AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Join_202_left')}}

),

AlteryxSelect_407 AS (

  SELECT 
    SOURCE_SKU AS SOURCE_SKU,
    FILENAME AS FILENAME
  
  FROM Join_202_left AS in0

),

Join_339_left AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Join_339_left')}}

),

Formula_417_0 AS (

  SELECT 
    CAST('Items from Inventory, Sales, Production or Transfers which don\'t have a match in the 13 week sales file.' AS STRING) AS DQ_ISSUE,
    CAST('Adjust 13 week sales file to account for these SKUs or figure out how to move the SKUs so they don\'t expire.' AS STRING) AS ACTION,
    *
  
  FROM Join_339_left AS in0

),

Union_395_reformat_9 AS (

  SELECT 
    ACTION AS ACTION,
    CAST(BEGINNING_INVENTORY AS FLOAT) AS BEGINNING_INVENTORY,
    (TO_CHAR(DATE_OF_EVENT, 'YYYY-MM-DD')) AS DATE_OF_EVENT,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(ENDING_INVENTORY AS FLOAT) AS ENDING_INVENTORY,
    CAST(PRODUCTION_QTY AS FLOAT) AS PRODUCTION_QTY,
    CAST(ROWSORTTIER AS STRING) AS ROWSORTTIER,
    CAST(ROWTYPE AS STRING) AS ROWTYPE,
    CAST(SALES_QTY AS FLOAT) AS SALES_QTY,
    CAST(SKU_STANDARD AS STRING) AS SKU_STANDARD,
    CAST(TRANSFER_QTY AS FLOAT) AS TRANSFER_QTY,
    CAST(WH_ID_STANDARD AS STRING) AS WH_ID_STANDARD
  
  FROM Formula_417_0 AS in0

),

Formula_347_1 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Formula_347_1')}}

),

AlteryxSelect_551 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_551')}}

),

Join_553_right AS (

  SELECT in0.*
  
  FROM AlteryxSelect_551 AS in0
  LEFT JOIN Formula_347_1 AS in1
     ON (in1.SKU_STANDARD = in0.SKU_STANDARD)

),

Formula_564_0 AS (

  SELECT 
    CAST('Items which have a sales forecast, but no inventory, production, transfers, or orders' AS STRING) AS DQ_ISSUE,
    CAST('Check to see if inventory needs to be produced or if there is a missing entry in the Item Master.' AS STRING) AS ACTION,
    *
  
  FROM Join_553_right AS in0

),

Union_395_reformat_3 AS (

  SELECT 
    ACTION AS ACTION,
    DQ_ISSUE AS DQ_ISSUE,
    CAST("FCST_2021-09-01" AS STRING) AS "FCST_2021-09-01",
    CAST("FCST_2021-10-01" AS STRING) AS "FCST_2021-10-01",
    CAST("FCST_2021-11-01" AS STRING) AS "FCST_2021-11-01",
    CAST("FCST_2021-12-01" AS STRING) AS "FCST_2021-12-01",
    CAST("FCST_2022-01-01" AS STRING) AS "FCST_2022-01-01",
    CAST("FCST_2022-02-01" AS STRING) AS "FCST_2022-02-01",
    CAST("FCST_2022-03-01" AS STRING) AS "FCST_2022-03-01",
    CAST("FCST_2022-04-01" AS STRING) AS "FCST_2022-04-01",
    CAST(SKU_STANDARD AS STRING) AS SKU_STANDARD
  
  FROM Formula_564_0 AS in0

),

Formula_412_0 AS (

  SELECT 
    CAST('Items from 13 week sales that don\'t have a match in Inventory, Sales, Production or Transfers.' AS STRING) AS DQ_ISSUE,
    CAST('Figure out why we don\'t have any sales or inventory for the SKU if we have a recent sales history and make adjustments to input files/inventory balances appropriately.' AS STRING) AS ACTION,
    *
  
  FROM Join_339_right AS in0

),

Union_395_reformat_7 AS (

  SELECT 
    "13_WK_WKLY_AVG_SALES" AS "13_WK_WKLY_AVG_SALES",
    ACTION AS ACTION,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(SKU_STANDARD AS STRING) AS SKU_STANDARD
  
  FROM Formula_412_0 AS in0

),

Join_195_left AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___Join_195_left')}}

),

Formula_406_0 AS (

  SELECT 
    CAST('Warehouse from Input File is not found in WH Master\'s SOURCE_WH_DESC field' AS STRING) AS DQ_ISSUE,
    CAST('Either update the input file to use a standard warehouse ID or update the warehouse master to recognize the warehouse from the input file by adding a record to SOURCE_WH_DESC' AS STRING) AS ACTION,
    *
  
  FROM Join_195_left AS in0

),

Union_395_reformat_4 AS (

  SELECT 
    ACTION AS ACTION,
    CAST(CUSTOMER AS STRING) AS CUSTOMER,
    1 AS DATE_OF_EVENT,
    1 AS DELIVERY_DT,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(FILENAME AS STRING) AS FILENAME,
    CAST(PICKUP_OR_DELIVERY AS STRING) AS PICKUP_OR_DELIVERY,
    CAST(PO_NUMBER AS STRING) AS PO_NUMBER,
    CAST(PRODUCTION_OR_TRANSFER AS STRING) AS PRODUCTION_OR_TRANSFER,
    CAST(QTY AS STRING) AS QTY,
    CAST(ROWSORTTIER AS STRING) AS ROWSORTTIER,
    CAST(ROWTYPE AS STRING) AS ROWTYPE,
    CAST(SALES_ORDER AS STRING) AS SALES_ORDER,
    1 AS SHIP_DT,
    CAST(SOURCE_SKU AS STRING) AS SOURCE_SKU,
    CAST(SOURCE_WH_DESC AS STRING) AS SOURCE_WH_DESC
  
  FROM Formula_406_0 AS in0

),

AlteryxSelect_533 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4_1___AlteryxSelect_533')}}

),

AlteryxSelect_558 AS (

  SELECT FILENAME AS FILENAME
  
  FROM AlteryxSelect_533 AS in0

),

SelectRecords_559_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['AlteryxSelect_558'], 
      'incremental_id', 
      'ROW_NUMBER', 
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

SelectRecords_559 AS (

  SELECT * 
  
  FROM SelectRecords_559_rowNumber AS in0
  
  WHERE (ROW_NUMBER = 1)

),

SelectRecords_559_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_559 AS in0

),

AlteryxSelect_555 AS (

  SELECT SKU AS SOURCE_SKU
  
  FROM Join_540_left AS in0

),

AppendFields_561 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM SelectRecords_559_cleanup_0 AS in0
  INNER JOIN AlteryxSelect_555 AS in1
     ON TRUE

),

Union_562 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_407', 'AppendFields_561'], 
      [
        '[{"name": "SOURCE_SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}]', 
        '[{"name": "FILENAME", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_409_0 AS (

  SELECT 
    CAST('SKU from Input File is not found in Item Master\'s SOURCE_SKU field' AS STRING) AS DQ_ISSUE,
    CAST('Either update the input file to use a standard SKU or update the item master to recognize the SKU from the input file by adding a record to SOURCE_SKU, SKU_STANDARD, and SKU_DESC_STANDARD' AS STRING) AS ACTION,
    *
  
  FROM Union_562 AS in0

),

Union_395_reformat_6 AS (

  SELECT 
    ACTION AS ACTION,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(FILENAME AS STRING) AS FILENAME,
    CAST(SOURCE_SKU AS STRING) AS SOURCE_SKU
  
  FROM Formula_409_0 AS in0

),

Union_395_reformat_8 AS (

  SELECT 
    ACTION AS ACTION,
    CAST(CUSTOMER AS STRING) AS CUSTOMER,
    (TO_CHAR(DATE_OF_EVENT, 'YYYY-MM-DD')) AS DATE_OF_EVENT,
    (TO_CHAR(DELIVERY_DT, 'YYYY-MM-DD')) AS DELIVERY_DT,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(FILENAME AS STRING) AS FILENAME,
    CAST(PICKUP_OR_DELIVERY AS STRING) AS PICKUP_OR_DELIVERY,
    CAST(PO_NUMBER AS STRING) AS PO_NUMBER,
    CAST(PRODUCTION_OR_TRANSFER AS STRING) AS PRODUCTION_OR_TRANSFER,
    CAST(QTY AS STRING) AS QTY,
    CAST(ROWSORTTIER AS STRING) AS ROWSORTTIER,
    CAST(ROWTYPE AS STRING) AS ROWTYPE,
    CAST(SALES_ORDER AS STRING) AS SALES_ORDER,
    (TO_CHAR(SHIP_DT, 'YYYY-MM-DD')) AS SHIP_DT,
    CAST(SKU_CATEGORY AS STRING) AS SKU_CATEGORY,
    CAST(SKU_DESC_STANDARD AS STRING) AS SKU_DESC_STANDARD,
    CAST(SKU_STANDARD AS STRING) AS SKU_STANDARD,
    CAST(SOURCE_SKU AS STRING) AS SOURCE_SKU,
    CAST(SOURCE_SKU_DESC AS STRING) AS SOURCE_SKU_DESC,
    CAST(SOURCE_WH_DESC AS STRING) AS SOURCE_WH_DESC,
    CAST(WH_DESC_STANDARD AS STRING) AS WH_DESC_STANDARD,
    CAST(WH_ID_STANDARD AS STRING) AS WH_ID_STANDARD
  
  FROM Formula_604_0 AS in0

),

AlteryxSelect_428 AS (

  SELECT 
    DQ_ISSUE AS DQ_ISSUE,
    ACTION AS ACTION,
    SKU_STANDARD AS SKU_STANDARD
  
  FROM Formula_429_0 AS in0

),

Union_395_reformat_2 AS (

  SELECT 
    ACTION AS ACTION,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(SKU_STANDARD AS STRING) AS SKU_STANDARD
  
  FROM AlteryxSelect_428 AS in0

),

Union_395_reformat_1 AS (

  SELECT 
    ACTION AS ACTION,
    DQ_ISSUE AS DQ_ISSUE,
    CAST(SOURCE_SKU AS STRING) AS SOURCE_SKU
  
  FROM AlteryxSelect_400 AS in0

),

Union_395 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Union_395_reformat_8', 
        'Union_395_reformat_0', 
        'Union_395_reformat_4', 
        'Union_395_reformat_2', 
        'Union_395_reformat_11', 
        'Union_395_reformat_1', 
        'Union_395_reformat_5', 
        'Union_395_reformat_10', 
        'Union_395_reformat_7', 
        'Union_395_reformat_6', 
        'Union_395_reformat_3', 
        'Union_395_reformat_9'
      ], 
      [
        '[{"name": "ACTION", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "DATE_OF_EVENT", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "QTY", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "String"}, {"name": "SKU_CATEGORY", "dataType": "String"}, {"name": "SKU_DESC_STANDARD", "dataType": "String"}, {"name": "SKU_STANDARD", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}, {"name": "SOURCE_SKU_DESC", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "WH_DESC_STANDARD", "dataType": "String"}, {"name": "WH_ID_STANDARD", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "DATE_OF_EVENT", "dataType": "Number"}, {"name": "DELIVERY_DT", "dataType": "Number"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "QTY", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "Number"}, {"name": "SOURCE_SKU", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "SKU_STANDARD", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "DATE_OF_EVENT", "dataType": "Number"}, {"name": "DELIVERY_DT", "dataType": "Number"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "QTY", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "Number"}, {"name": "SKU", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "SKU_CATEGORY", "dataType": "String"}, {"name": "SKU_DESC_STANDARD", "dataType": "String"}, {"name": "SKU_STANDARD", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}, {"name": "SOURCE_SKU_DESC", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "ALERT_PROJECTED_SHORT_IN_WH_FOR_ITEM", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "SKU_STANDARD", "dataType": "String"}, {"name": "WH_ID_STANDARD", "dataType": "String"}]', 
        '[{"name": "13_WK_WKLY_AVG_SALES", "dataType": "Number"}, {"name": "ACTION", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "SKU_STANDARD", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "FCST_2021-09-01", "dataType": "String"}, {"name": "FCST_2021-10-01", "dataType": "String"}, {"name": "FCST_2021-11-01", "dataType": "String"}, {"name": "FCST_2021-12-01", "dataType": "String"}, {"name": "FCST_2022-01-01", "dataType": "String"}, {"name": "FCST_2022-02-01", "dataType": "String"}, {"name": "FCST_2022-03-01", "dataType": "String"}, {"name": "FCST_2022-04-01", "dataType": "String"}, {"name": "SKU_STANDARD", "dataType": "String"}]', 
        '[{"name": "ACTION", "dataType": "String"}, {"name": "BEGINNING_INVENTORY", "dataType": "Float"}, {"name": "DATE_OF_EVENT", "dataType": "String"}, {"name": "DQ_ISSUE", "dataType": "String"}, {"name": "ENDING_INVENTORY", "dataType": "Float"}, {"name": "PRODUCTION_QTY", "dataType": "Float"}, {"name": "ROWSORTTIER", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SALES_QTY", "dataType": "Float"}, {"name": "SKU_STANDARD", "dataType": "String"}, {"name": "TRANSFER_QTY", "dataType": "Float"}, {"name": "WH_ID_STANDARD", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_410 AS (

  SELECT 
    DQ_ISSUE AS DQ_ISSUE,
    ACTION AS ACTION,
    SOURCE_SKU AS SOURCE_SKU,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU_STANDARD AS SKU_STANDARD,
    "13_WK_WKLY_AVG_SALES" AS "13_WK_WKLY_AVG_SALES",
    FILENAME AS FILENAME,
    DATE_OF_EVENT AS DATE_OF_EVENT,
    CUSTOMER AS CUSTOMER,
    SALES_ORDER AS SALES_ORDER,
    PO_NUMBER AS PO_NUMBER,
    PICKUP_OR_DELIVERY AS PICKUP_OR_DELIVERY,
    SHIP_DT AS SHIP_DT,
    DELIVERY_DT AS DELIVERY_DT,
    SKU AS SKU,
    QTY AS QTY,
    WH_ID_STANDARD AS WH_ID_STANDARD,
    WH_DESC_STANDARD AS WH_DESC_STANDARD,
    SKU_DESC_STANDARD AS SKU_DESC_STANDARD,
    ROWTYPE AS ROWTYPE,
    ROWSORTTIER AS ROWSORTTIER,
    PRODUCTION_OR_TRANSFER AS PRODUCTION_OR_TRANSFER,
    SOURCE_SKU_DESC AS SOURCE_SKU_DESC,
    SKU_CATEGORY AS SKU_CATEGORY,
    * EXCLUDE ("DQ_ISSUE", 
    "ACTION", 
    "SOURCE_SKU", 
    "SOURCE_WH_DESC", 
    "SKU_STANDARD", 
    "13_WK_WKLY_AVG_SALES", 
    "FILENAME", 
    "DATE_OF_EVENT", 
    "CUSTOMER", 
    "SALES_ORDER", 
    "PO_NUMBER", 
    "PICKUP_OR_DELIVERY", 
    "SHIP_DT", 
    "DELIVERY_DT", 
    "SKU", 
    "QTY", 
    "WH_ID_STANDARD", 
    "WH_DESC_STANDARD", 
    "SKU_DESC_STANDARD", 
    "ROWTYPE", 
    "ROWSORTTIER", 
    "PRODUCTION_OR_TRANSFER", 
    "SOURCE_SKU_DESC", 
    "SKU_CATEGORY")
  
  FROM Union_395 AS in0

),

AlteryxSelect_422 AS (

  SELECT 
    DQ_ISSUE AS ALERT,
    ACTION AS ACTION,
    FILENAME AS FILENAME,
    SOURCE_SKU AS SOURCE_SKU,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU_STANDARD AS SKU_STANDARD,
    "13_WK_WKLY_AVG_SALES" AS "13_WK_WKLY_AVG_SALES",
    * EXCLUDE ("ACTION", "FILENAME", "SOURCE_SKU", "SOURCE_WH_DESC", "SKU_STANDARD", "13_WK_WKLY_AVG_SALES", "DQ_ISSUE")
  
  FROM AlteryxSelect_410 AS in0

)

SELECT *

FROM AlteryxSelect_422
