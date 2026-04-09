{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Union_160 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Union_160')}}

),

Filter_571 AS (

  SELECT * 
  
  FROM Union_160 AS in0
  
  WHERE (
          (
            (
              NOT(
                QTY = 0)
            ) OR (QTY IS NULL)
          ) AND (NOT(QTY IS NULL))
        )

),

Formula_178_0 AS (

  SELECT 
    CURRENT_DATE AS DATE_OF_EVENT,
    CAST('1_BEG_INVENTORY' AS STRING) AS ROWTYPE,
    1 AS ROWSORTTIER,
    *
  
  FROM Filter_571 AS in0

),

Cleanse_219 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_178_0'], 
      [
        { "name": "DATE_OF_EVENT", "dataType": "Date" }, 
        { "name": "FILENAME", "dataType": "String" }, 
        { "name": "SKU", "dataType": "String" }, 
        { "name": "QTY", "dataType": "Double" }, 
        { "name": "ROWTYPE", "dataType": "String" }, 
        { "name": "SOURCE_WH_DESC", "dataType": "String" }, 
        { "name": "ROWSORTTIER", "dataType": "Integer" }
      ], 
      'keepOriginal', 
      ['QTY'], 
      true, 
      '', 
      true, 
      0, 
      false, 
      false, 
      false, 
      true, 
      true, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

AlteryxSelect_193 AS (

  SELECT 
    CAST(QTY AS INTEGER) AS QTY,
    * EXCLUDE ("QTY")
  
  FROM Cleanse_219 AS in0

),

DbFileInput_297_297 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'DbFileInput_297_297') }}

),

Filter_299 AS (

  SELECT * 
  
  FROM DbFileInput_297_297 AS in0
  
  WHERE (NOT(F2 IS NULL))

),

DynamicRename_298 AS (

  SELECT 
    "REAL GOOD FOOD COMPANY" AS FIELD_26,
    F2 AS VARIABLEDATE,
    F3 AS NUM,
    F4 AS VENDOR,
    F5 AS PRODUCTSLASHSERVICE,
    F6 AS ACCOUNT,
    F7 AS QTY,
    F8 AS "RECEIVED QTY",
    F9 AS "BACKORDERED QTY",
    F10 AS "TOTAL AMT",
    F11 AS "RECEIVED AMT",
    F12 AS "OPEN BALANCE",
    F13 AS CUSTOMER,
    F14 AS CLASS,
    F15 AS SKU,
    F16 AS "CREATE DATE",
    F17 AS "CREATED BY",
    F18 AS "LAST MODIFIED",
    F19 AS "LAST MODIFIED BY",
    F20 AS "CLIENTSLASHVENDOR MESSAGE",
    F21 AS MEMOSLASHDESCRIPTION,
    F22 AS RATE,
    F23 AS "TAX NAME",
    F24 AS "TAX AMOUNT",
    F25 AS "TAXABLE AMOUNT"
  
  FROM Filter_299 AS in0

),

DynamicRename_298_row_number AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_298'], 
      'incremental_id', 
      'PROPHECY_ROW_ID', 
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

DynamicRename_298_filter AS (

  SELECT * 
  
  FROM DynamicRename_298_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR (PROPHECY_ROW_ID IS NULL)
        )

),

DynamicRename_298_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_298_filter AS in0

),

Formula_301_0 AS (

  SELECT 
    (
      CASE
        WHEN (CONTAINS((coalesce(LOWER(VENDOR), '')), LOWER('Production - ')))
          THEN 'Production'
        WHEN (CONTAINS((coalesce(LOWER(VENDOR), '')), LOWER('Transfer - ')))
          THEN 'Transfer  '
        ELSE NULL
      END
    ) AS PRODUCTION_OR_TRANSFER_FLAG,
    CAST((datepart('' epoch_second '', (TO_TIMESTAMP((REGEXP_REPLACE(VARIABLEDATE, '\\.\\d+', '')), 'MM/DD/YY')))) AS STRING) AS VARIABLEDATE,
    * EXCLUDE ("VARIABLEDATE")
  
  FROM DynamicRename_298_drop_0 AS in0

),

Filter_302 AS (

  SELECT * 
  
  FROM Formula_301_0 AS in0
  
  WHERE (NOT(PRODUCTION_OR_TRANSFER_FLAG IS NULL))

),

AlteryxSelect_308 AS (

  SELECT 
    NUM AS PO_NUMBER,
    CAST(QTY AS INTEGER) AS QTY,
    * EXCLUDE ("QTY", "NUM")
  
  FROM Filter_302 AS in0

),

Formula_303_0 AS (

  SELECT 
    CAST(TRIM((REGEXP_REPLACE((REGEXP_REPLACE(VENDOR, 'Production -', '')), 'Transfer - ', ''))) AS STRING) AS SOURCE_WH_DESC,
    CAST('4_5 Transfer and Production PO Export' AS STRING) AS FILENAME,
    (
      CASE
        WHEN ((SUBSTRING("CLIENTSLASHVENDOR MESSAGE", 1, 2)) = 'F,')
          THEN (SUBSTRING("CLIENTSLASHVENDOR MESSAGE", 3, 4))
        ELSE NULL
      END
    ) AS FROM_WH_ID,
    CAST('2_TRANSFER_OR_PRODUCTION' AS STRING) AS ROWTYPE,
    2 AS ROWSORTTIER,
    *
  
  FROM AlteryxSelect_308 AS in0

),

Filter_496_reject AS (

  SELECT * 
  
  FROM Formula_303_0 AS in0
  
  WHERE (
          (
            NOT(
              PRODUCTION_OR_TRANSFER_FLAG = 'Production')
          )
          OR ((PRODUCTION_OR_TRANSFER_FLAG = 'Production') IS NULL)
        )

),

Formula_282_0 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Formula_282_0')}}

),

Union_191_reformat_2 AS (

  SELECT 
    CAST(CUSTOMER AS STRING) AS CUSTOMER,
    DATE_OF_EVENT AS DATE_OF_EVENT,
    (TO_CHAR(DELIVERY_DT, 'YYYY-MM-DD')) AS DELIVERY_DT,
    FILENAME AS FILENAME,
    CAST(PICKUP_OR_DELIVERY AS STRING) AS PICKUP_OR_DELIVERY,
    CAST(PO_NUMBER AS STRING) AS PO_NUMBER,
    CAST(QTY AS DOUBLE) AS QTY,
    ROWSORTTIER AS ROWSORTTIER,
    ROWTYPE AS ROWTYPE,
    CAST(SALES_ORDER AS STRING) AS SALES_ORDER,
    (TO_CHAR(SHIP_DT, 'YYYY-MM-DD')) AS SHIP_DT,
    SKU AS SKU,
    SOURCE_WH_DESC AS SOURCE_WH_DESC
  
  FROM Formula_282_0 AS in0

),

TextToColumns_498 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Filter_496_reject'], 
      'CLIENTSLASHVENDOR MESSAGE', 
      ",", 
      'splitColumns', 
      6, 
      'leaveExtraCharLastCol', 
      'CLIENTSLASHVENDOR MESSAGE', 
      'CLIENTSLASHVENDOR MESSAGE', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_498_dropGem_0 AS (

  SELECT 
    "CLIENTSLASHVENDOR MESSAGE_1_CLIENTSLASHVENDOR MESSAGE" AS "CLIENTSLASHVENDOR MESSAGE1",
    "CLIENTSLASHVENDOR MESSAGE_2_CLIENTSLASHVENDOR MESSAGE" AS "CLIENTSLASHVENDOR MESSAGE2",
    "CLIENTSLASHVENDOR MESSAGE_3_CLIENTSLASHVENDOR MESSAGE" AS "CLIENTSLASHVENDOR MESSAGE3",
    "CLIENTSLASHVENDOR MESSAGE_4_CLIENTSLASHVENDOR MESSAGE" AS "CLIENTSLASHVENDOR MESSAGE4",
    "CLIENTSLASHVENDOR MESSAGE_5_CLIENTSLASHVENDOR MESSAGE" AS "CLIENTSLASHVENDOR MESSAGE5",
    "CLIENTSLASHVENDOR MESSAGE_6_CLIENTSLASHVENDOR MESSAGE" AS "CLIENTSLASHVENDOR MESSAGE6",
    *
  
  FROM TextToColumns_498 AS in0

),

Formula_499_0 AS (

  SELECT 
    CAST(date_part(
      'EPOCH_SECOND', 
      to_timestamp(regexp_replace("" CLIENTSLASHVENDOR MESSAGE3 "", '\\.\\d+', ''), 'MMddyy')) AS string) AS VARIABLEDATE,
    CAST("CLIENTSLASHVENDOR MESSAGE2" AS STRING) AS SOURCE_WH_DESC,
    (-1 * QTY) AS QTY,
    * EXCLUDE ("VARIABLEDATE", "SOURCE_WH_DESC", "QTY")
  
  FROM TextToColumns_498_dropGem_0 AS in0

),

Formula_499_1 AS (

  SELECT 
    (TO_CHAR(VARIABLEDATE, 'YYYY-MM-DD')) AS SHIP_DT,
    to_char(
      date_part(
        'EPOCH_SECOND', 
        to_timestamp(regexp_replace("" CLIENTSLASHVENDOR MESSAGE6 "", '\\.\\d+', ''), 'MMddyy')), 
      'YYYY-MM-DD') AS DELIVERY_DT,
    *
  
  FROM Formula_499_0 AS in0

),

Formula_307_0 AS (

  SELECT 
    CAST("CLIENTSLASHVENDOR MESSAGE5" AS STRING) AS SOURCE_WH_DESC,
    (-1 * QTY) AS QTY,
    CAST(date_part(
      'EPOCH_SECOND', 
      to_timestamp(regexp_replace("" CLIENTSLASHVENDOR MESSAGE6 "", '\\.\\d+', ''), 'MMddyy')) AS string) AS VARIABLEDATE,
    * EXCLUDE ("VARIABLEDATE", "SOURCE_WH_DESC", "QTY")
  
  FROM Formula_499_1 AS in0

),

Filter_496 AS (

  SELECT * 
  
  FROM Formula_303_0 AS in0
  
  WHERE (PRODUCTION_OR_TRANSFER_FLAG = 'Production')

),

Union_309 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_307_0', 'Filter_496', 'Formula_499_1'], 
      [
        '[{"name": "RATE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_1_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER_FLAG", "dataType": "String"}, {"name": "TOTAL AMT", "dataType": "String"}, {"name": "CREATED BY", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE1", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE4", "dataType": "String"}, {"name": "PRODUCTSLASHSERVICE", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "FROM_WH_ID", "dataType": "String"}, {"name": "ACCOUNT", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "TAX NAME", "dataType": "String"}, {"name": "LAST MODIFIED BY", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_6_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE3", "dataType": "String"}, {"name": "RECEIVED AMT", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_5_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "VENDOR", "dataType": "String"}, {"name": "RECEIVED QTY", "dataType": "String"}, {"name": "LAST MODIFIED", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE6", "dataType": "String"}, {"name": "TAXABLE AMOUNT", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "OPEN BALANCE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_2_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "Date"}, {"name": "CLIENTSLASHVENDOR MESSAGE2", "dataType": "String"}, {"name": "QTY", "dataType": "Integer"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "TAX AMOUNT", "dataType": "String"}, {"name": "BACKORDERED QTY", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "Date"}, {"name": "CLIENTSLASHVENDOR MESSAGE_3_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_4_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLASS", "dataType": "String"}, {"name": "MEMOSLASHDESCRIPTION", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE5", "dataType": "String"}, {"name": "FIELD_26", "dataType": "String"}, {"name": "CREATE DATE", "dataType": "String"}, {"name": "VARIABLEDATE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Integer"}]', 
        '[{"name": "RATE", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER_FLAG", "dataType": "String"}, {"name": "TOTAL AMT", "dataType": "String"}, {"name": "CREATED BY", "dataType": "String"}, {"name": "PRODUCTSLASHSERVICE", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "FROM_WH_ID", "dataType": "String"}, {"name": "ACCOUNT", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "TAX NAME", "dataType": "String"}, {"name": "LAST MODIFIED BY", "dataType": "String"}, {"name": "RECEIVED AMT", "dataType": "String"}, {"name": "VENDOR", "dataType": "String"}, {"name": "RECEIVED QTY", "dataType": "String"}, {"name": "LAST MODIFIED", "dataType": "String"}, {"name": "TAXABLE AMOUNT", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "OPEN BALANCE", "dataType": "String"}, {"name": "QTY", "dataType": "Integer"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "TAX AMOUNT", "dataType": "String"}, {"name": "BACKORDERED QTY", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "CLASS", "dataType": "String"}, {"name": "MEMOSLASHDESCRIPTION", "dataType": "String"}, {"name": "FIELD_26", "dataType": "String"}, {"name": "CREATE DATE", "dataType": "String"}, {"name": "VARIABLEDATE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Integer"}]', 
        '[{"name": "RATE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_1_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER_FLAG", "dataType": "String"}, {"name": "TOTAL AMT", "dataType": "String"}, {"name": "CREATED BY", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE1", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE4", "dataType": "String"}, {"name": "PRODUCTSLASHSERVICE", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "FROM_WH_ID", "dataType": "String"}, {"name": "ACCOUNT", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "TAX NAME", "dataType": "String"}, {"name": "LAST MODIFIED BY", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_6_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE3", "dataType": "String"}, {"name": "RECEIVED AMT", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_5_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "VENDOR", "dataType": "String"}, {"name": "RECEIVED QTY", "dataType": "String"}, {"name": "LAST MODIFIED", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE6", "dataType": "String"}, {"name": "TAXABLE AMOUNT", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "OPEN BALANCE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_2_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "Date"}, {"name": "CLIENTSLASHVENDOR MESSAGE2", "dataType": "String"}, {"name": "QTY", "dataType": "Integer"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "TAX AMOUNT", "dataType": "String"}, {"name": "BACKORDERED QTY", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "Date"}, {"name": "CLIENTSLASHVENDOR MESSAGE_3_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_4_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLASS", "dataType": "String"}, {"name": "MEMOSLASHDESCRIPTION", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE5", "dataType": "String"}, {"name": "FIELD_26", "dataType": "String"}, {"name": "CREATE DATE", "dataType": "String"}, {"name": "VARIABLEDATE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_300 AS (

  SELECT 
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    CAST((TRY_TO_TIMESTAMP(CAST(VARIABLEDATE AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS DATE) AS DATE_OF_EVENT,
    PO_NUMBER AS PO_NUMBER,
    SKU AS SKU,
    QTY AS QTY,
    PRODUCTION_OR_TRANSFER_FLAG AS PRODUCTION_OR_TRANSFER,
    ROWTYPE AS ROWTYPE,
    ROWSORTTIER AS ROWSORTTIER,
    FILENAME AS FILENAME,
    SHIP_DT AS SHIP_DT,
    DELIVERY_DT AS DELIVERY_DT
  
  FROM Union_309 AS in0

),

Union_191_reformat_1 AS (

  SELECT 
    DATE_OF_EVENT AS DATE_OF_EVENT,
    (TO_CHAR(DELIVERY_DT, 'YYYY-MM-DD')) AS DELIVERY_DT,
    FILENAME AS FILENAME,
    CAST(PO_NUMBER AS STRING) AS PO_NUMBER,
    CAST(PRODUCTION_OR_TRANSFER AS STRING) AS PRODUCTION_OR_TRANSFER,
    QTY AS QTY,
    ROWSORTTIER AS ROWSORTTIER,
    ROWTYPE AS ROWTYPE,
    (TO_CHAR(SHIP_DT, 'YYYY-MM-DD')) AS SHIP_DT,
    SKU AS SKU,
    SOURCE_WH_DESC AS SOURCE_WH_DESC
  
  FROM AlteryxSelect_300 AS in0

),

Union_191_reformat_0 AS (

  SELECT 
    DATE_OF_EVENT AS DATE_OF_EVENT,
    FILENAME AS FILENAME,
    CAST(QTY AS DOUBLE) AS QTY,
    ROWSORTTIER AS ROWSORTTIER,
    ROWTYPE AS ROWTYPE,
    SKU AS SKU,
    SOURCE_WH_DESC AS SOURCE_WH_DESC
  
  FROM AlteryxSelect_193 AS in0

),

Union_191 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_191_reformat_0', 'Union_191_reformat_2', 'Union_191_reformat_1'], 
      [
        '[{"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "FILENAME", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "QTY", "dataType": "Integer"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Integer"}]', 
        '[{"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "FILENAME", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "Date"}, {"name": "QTY", "dataType": "Integer"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "Date"}, {"name": "ROWSORTTIER", "dataType": "Integer"}]', 
        '[{"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "FILENAME", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "Date"}, {"name": "QTY", "dataType": "Double"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "Date"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_204 AS (

  SELECT 
    SKU AS SOURCE_SKU,
    * EXCLUDE ("SKU")
  
  FROM Union_191 AS in0

),

Cleanse_317 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_204'], 
      [
        { "name": "DATE_OF_EVENT", "dataType": "Date" }, 
        { "name": "FILENAME", "dataType": "String" }, 
        { "name": "SALES_ORDER", "dataType": "String" }, 
        { "name": "SOURCE_SKU", "dataType": "String" }, 
        { "name": "CUSTOMER", "dataType": "String" }, 
        { "name": "DELIVERY_DT", "dataType": "Date" }, 
        { "name": "QTY", "dataType": "Double" }, 
        { "name": "ROWTYPE", "dataType": "String" }, 
        { "name": "PO_NUMBER", "dataType": "String" }, 
        { "name": "PICKUP_OR_DELIVERY", "dataType": "String" }, 
        { "name": "SOURCE_WH_DESC", "dataType": "String" }, 
        { "name": "SHIP_DT", "dataType": "Date" }, 
        { "name": "PRODUCTION_OR_TRANSFER", "dataType": "String" }, 
        { "name": "ROWSORTTIER", "dataType": "Integer" }
      ], 
      'makeUppercase', 
      ['SOURCE_WH_DESC', 'SOURCE_SKU'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

)

SELECT *

FROM Cleanse_317
