{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH WarehouseMaster_129 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'WarehouseMaster_129') }}

),

AlteryxSelect_130 AS (

  {#VisualGroup: INPUTITEMMASTER#}
  SELECT 
    CAST(WH_ID_STANDARD AS STRING) AS WH_ID_STANDARD,
    * EXCLUDE ("WH_ID_STANDARD")
  
  FROM WarehouseMaster_129 AS in0

),

Cleanse_316 AS (

  {#VisualGroup: INPUTITEMMASTER#}
  {{ prophecy_basics.DataCleansing(['AlteryxSelect_130'],[{"name": "SOURCEWHDESCRIPTION", "dataType": "String"}, {"name": "DATE_OF_EVENT", "dataType": "String"}, {"name": "SKU_CATEGORY", "dataType": "String"}, {"name": "WH_ID_STANDARD", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "DUPLICATE SOURCE_WH_DESC IN WH MASTER FILE", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "String"}, {"name": "QTY", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "FILE", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "String"}, {"name": "WH_DESC_STANDARD", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "SKU_DESC_STANDARD", "dataType": "String"}, {"name": "SOURCE_SKU_DESC", "dataType": "String"}, {"name": "SKU_STANDARD", "dataType": "String"}, {"name": ""13_WK_WKLY_AVG_SALES"", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "String"}, {"name": "REMOVE THE DUPLICATE SOURCE_WH_DESC IN THE WH MASTER FILE", "dataType": "String"}],'makeUppercase',['SOURCEWHDESCRIPTION'],false,'',false,0,true,false,false,false,false,false,false,false,'1970-01-01',false,'1970-01-01 00:00:00.0') }}

)

SELECT *

FROM Cleanse_316
