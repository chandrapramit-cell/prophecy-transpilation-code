{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Join_195_left AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Join_195_left')}}

),

Cleanse_316 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Cleanse_316')}}

),

Cleanse_317 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Cleanse_317')}}

),

Join_195_inner AS (

  SELECT 
    in0.SOURCE_SKU AS SOURCE_SKU,
    in1.WH_DESC_STANDARD AS WH_DESC_STANDARD,
    in0.PICKUP_OR_DELIVERY AS PICKUP_OR_DELIVERY,
    in0.DATE_OF_EVENT AS DATE_OF_EVENT,
    in0.PRODUCTION_OR_TRANSFER AS PRODUCTION_OR_TRANSFER,
    in0.ROWTYPE AS ROWTYPE,
    in0.DELIVERY_DT AS DELIVERY_DT,
    in1.WH_ID_STANDARD AS WH_ID_STANDARD,
    in0.PO_NUMBER AS PO_NUMBER,
    in0.FILENAME AS FILENAME,
    in0.CUSTOMER AS CUSTOMER,
    in0.QTY AS QTY,
    in0.SALES_ORDER AS SALES_ORDER,
    in0.SHIP_DT AS SHIP_DT,
    in0.SOURCE_WH_DESC AS SOURCE_WH_DESC,
    in0.ROWSORTTIER AS ROWSORTTIER
  
  FROM Cleanse_317 AS in0
  INNER JOIN Cleanse_316 AS in1
     ON (in0.SOURCE_WH_DESC = in1.SOURCEWHDESCRIPTION)

),

Union_198 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_195_inner', 'Join_195_left'], 
      [
        '[{"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "WH_ID_STANDARD", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "Date"}, {"name": "QTY", "dataType": "Double"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "Date"}, {"name": "WH_DESC_STANDARD", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Integer"}]', 
        '[{"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "FILENAME", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "Date"}, {"name": "QTY", "dataType": "Double"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "Date"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_198
