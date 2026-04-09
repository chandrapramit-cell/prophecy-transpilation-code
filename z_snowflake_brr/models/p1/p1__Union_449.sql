{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Join_202_left AS (

  SELECT *
  
  FROM {{ ref('p1__Join_202_left')}}

),

Formula_448_0 AS (

  SELECT 
    CAST(SOURCE_SKU AS STRING) AS SKU_STANDARD,
    *
  
  FROM Join_202_left AS in0

),

Union_198 AS (

  SELECT *
  
  FROM {{ ref('p1__Union_198')}}

),

Formula_215_0 AS (

  SELECT *
  
  FROM {{ ref('p1__Formula_215_0')}}

),

Join_202_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("SOURCE_SKU")
  
  FROM Union_198 AS in0
  INNER JOIN Formula_215_0 AS in1
     ON (in0.SOURCE_SKU = in1.SOURCE_SKU)

),

Union_449 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_202_inner', 'Formula_448_0'], 
      [
        '[{"name": "SOURCE_SKU", "dataType": "String"}, {"name": "WH_DESC_STANDARD", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "String"}, {"name": "WH_ID_STANDARD", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "QTY", "dataType": "Float"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Number"}, {"name": "SKU_STANDARD", "dataType": "String"}, {"name": "SOURCE_SKU_DESC", "dataType": "String"}, {"name": "SKU_DESC_STANDARD", "dataType": "String"}, {"name": "SKU_CATEGORY", "dataType": "String"}]', 
        '[{"name": "SKU_STANDARD", "dataType": "String"}, {"name": "SOURCE_SKU", "dataType": "String"}, {"name": "WH_DESC_STANDARD", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "String"}, {"name": "WH_ID_STANDARD", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "QTY", "dataType": "Float"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_449
