{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Join_339_left AS (

  SELECT *
  
  FROM {{ ref('p1__Join_339_left')}}

),

AlteryxSelect_338 AS (

  SELECT *
  
  FROM {{ ref('p1__AlteryxSelect_338')}}

),

Formula_237_0 AS (

  SELECT *
  
  FROM {{ ref('p1__Formula_237_0')}}

),

Join_339_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("SKU_STANDARD")
  
  FROM Formula_237_0 AS in0
  INNER JOIN AlteryxSelect_338 AS in1
     ON (in0.SKU_STANDARD = in1.SKU_STANDARD)

),

Union_345 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_339_inner', 'Join_339_left'], 
      [
        '[{"name": "ROWTYPE", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Number"}, {"name": "WH_ID_STANDARD", "dataType": "Number"}, {"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "BEGINNING_INVENTORY", "dataType": "Number"}, {"name": "SALES_QTY", "dataType": "Number"}, {"name": "PRODUCTION_QTY", "dataType": "Number"}, {"name": "TRANSFER_QTY", "dataType": "Number"}, {"name": "ENDING_INVENTORY", "dataType": "Float"}, {"name": "SKU_STANDARD", "dataType": "String"}, {"name": "13_WK_WKLY_AVG_SALES", "dataType": "Number"}]', 
        '[{"name": "ROWTYPE", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Number"}, {"name": "WH_ID_STANDARD", "dataType": "Number"}, {"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "BEGINNING_INVENTORY", "dataType": "Number"}, {"name": "SALES_QTY", "dataType": "Number"}, {"name": "PRODUCTION_QTY", "dataType": "Number"}, {"name": "TRANSFER_QTY", "dataType": "Number"}, {"name": "ENDING_INVENTORY", "dataType": "Float"}, {"name": "SKU_STANDARD", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_347_0 AS (

  SELECT 
    1 AS "13_WK_WKLY_AVG_SALES",
    * EXCLUDE ("13_WK_WKLY_AVG_SALES")
  
  FROM Union_345 AS in0

),

Formula_347_1 AS (

  SELECT 
    CAST((ENDING_INVENTORY / "13_WK_WKLY_AVG_SALES") AS INTEGER) AS WEEKS_ON_HAND_INV,
    *
  
  FROM Formula_347_0 AS in0

)

SELECT *

FROM Formula_347_1
