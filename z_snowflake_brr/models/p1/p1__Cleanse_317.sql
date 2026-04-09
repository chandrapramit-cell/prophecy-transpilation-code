{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DbFileInput_503_503 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'DbFileInput_503_503') }}

),

AlteryxSelect_512 AS (

  SELECT * EXCLUDE ("FILENAME")
  
  FROM DbFileInput_503_503 AS in0

),

SelectRecords_506_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['AlteryxSelect_512'], 
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

SelectRecords_506 AS (

  SELECT * 
  
  FROM SelectRecords_506_rowNumber AS in0
  
  WHERE (ROW_NUMBER >= 6)

),

SelectRecords_506_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_506 AS in0

),

DynamicRename_507 AS (

  SELECT 
    "DETAILED INVENTORY BY OWNER" AS LOT,
    F2 AS RECEIVED,
    F3 AS "PO NUMBER",
    F4 AS V___LOT,
    F5 AS MAN___DATE,
    F6 AS EXP___DATE,
    F7 AS FIELD_17,
    F8 AS "ACTIVE INVENTORY",
    F9 AS "DAMAGED INVENTORY",
    "PRINTED DATECOLON 9SLASH3SLASH2021" AS FIELD_17_4,
    F11 AS UOM,
    F12 AS HASHLP,
    F13 AS NET_WT__,
    F14 AS FIELD_17_2,
    F15 AS FIELD_17_3,
    F16 AS GROSS_WT__
  
  FROM SelectRecords_506_cleanup_0 AS in0

),

DynamicRename_507_row_number AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_507'], 
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

AlteryxSelect_513 AS (

  SELECT FILENAME AS FILENAME
  
  FROM DbFileInput_503_503 AS in0

),

SelectRecords_514_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['AlteryxSelect_513'], 
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

SelectRecords_514 AS (

  SELECT * 
  
  FROM SelectRecords_514_rowNumber AS in0
  
  WHERE (ROW_NUMBER = 1)

),

SelectRecords_514_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_514 AS in0

),

DynamicRename_507_filter AS (

  SELECT * 
  
  FROM DynamicRename_507_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR (PROPHECY_ROW_ID IS NULL)
        )

),

DynamicRename_507_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_507_filter AS in0

),

SelectRecords_508_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_507_drop_0'], 
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

SelectRecords_508 AS (

  SELECT * 
  
  FROM SelectRecords_508_rowNumber AS in0
  
  WHERE (ROW_NUMBER >= 2)

),

SelectRecords_508_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_508 AS in0

),

Filter_509 AS (

  SELECT * 
  
  FROM SelectRecords_508_cleanup_0 AS in0
  
  WHERE ((NOT((LOT IS NULL) OR ((LENGTH(LOT)) = 0))) AND ((RECEIVED IS NULL) OR ((LENGTH(RECEIVED)) = 0)))

),

Cleanse_511 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Filter_509'], 
      [
        { "name": "GROSS_WT__", "dataType": "String" }, 
        { "name": "FIELD_17", "dataType": "String" }, 
        { "name": "EXP___DATE", "dataType": "String" }, 
        { "name": "FIELD_17_2", "dataType": "String" }, 
        { "name": "RECEIVED", "dataType": "String" }, 
        { "name": "HASHLP", "dataType": "String" }, 
        { "name": "PO NUMBER", "dataType": "String" }, 
        { "name": "MAN___DATE", "dataType": "String" }, 
        { "name": "FIELD_17_3", "dataType": "String" }, 
        { "name": "DAMAGED INVENTORY", "dataType": "String" }, 
        { "name": "V___LOT", "dataType": "String" }, 
        { "name": "LOT", "dataType": "String" }, 
        { "name": "FIELD_17_4", "dataType": "String" }, 
        { "name": "ACTIVE INVENTORY", "dataType": "String" }, 
        { "name": "UOM", "dataType": "String" }, 
        { "name": "NET_WT__", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['ACTIVE INVENTORY'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      true, 
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

AppendFields_515 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM SelectRecords_514_cleanup_0 AS in0
  INNER JOIN Cleanse_511 AS in1
     ON TRUE

),

Formula_517_0 AS (

  SELECT 
    CAST('2005' AS STRING) AS SOURCE_WH_DESC,
    CAST(TRIM(LOT) AS STRING) AS LOT,
    * EXCLUDE ("LOT")
  
  FROM AppendFields_515 AS in0

),

AlteryxSelect_510 AS (

  SELECT 
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    LOT AS SKU,
    CAST("ACTIVE INVENTORY" AS INTEGER) AS QTY,
    FILENAME AS FILENAME
  
  FROM Formula_517_0 AS in0

),

Summarize_580 AS (

  SELECT 
    SUM(QTY) AS QTY,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU AS SKU,
    FILENAME AS FILENAME
  
  FROM AlteryxSelect_510 AS in0
  
  GROUP BY 
    SOURCE_WH_DESC, SKU, FILENAME

),

DbFileInput_127_127 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'DbFileInput_127_127') }}

),

AlteryxSelect_128 AS (

  SELECT 
    "FACILITY NAME" AS SOURCE_WH_DESC,
    "ITEM CODE" AS SKU,
    "ON HAND" AS QTY,
    FILENAME AS FILENAME
  
  FROM DbFileInput_127_127 AS in0

),

Lineage11_01_21_570 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'Lineage11_01_21_570') }}

),

AlteryxSelect_519 AS (

  SELECT 
    "DESCRIPTION SLASH INVENTORY DETAILS" AS SKU,
    FIELD_18 AS QTY,
    FILENAME AS FILENAME
  
  FROM Lineage11_01_21_570 AS in0

),

Filter_522 AS (

  SELECT * 
  
  FROM AlteryxSelect_519 AS in0
  
  WHERE (
          NOT(
            (LENGTH(SKU)) = 0)
        )

),

Formula_520_0 AS (

  SELECT 
    CAST('2006' AS STRING) AS SOURCE_WH_DESC,
    *
  
  FROM Filter_522 AS in0

),

Cleanse_524 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_520_0'], 
      [
        { "name": "SOURCE_WH_DESC", "dataType": "String" }, 
        { "name": "SKU", "dataType": "String" }, 
        { "name": "QTY", "dataType": "String" }, 
        { "name": "FILENAME", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['QTY'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      true, 
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

AlteryxSelect_525 AS (

  SELECT 
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    CAST(SKU AS STRING) AS SKU,
    CAST(QTY AS INTEGER) AS QTY,
    FILENAME AS FILENAME
  
  FROM Cleanse_524 AS in0

),

Summarize_523 AS (

  SELECT 
    SUM(QTY) AS QTY,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU AS SKU,
    FILENAME AS FILENAME
  
  FROM AlteryxSelect_525 AS in0
  
  GROUP BY 
    SOURCE_WH_DESC, SKU, FILENAME

),

RLS20211129_001_615 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'RLS20211129_001_615') }}

),

AlteryxSelect_616 AS (

  SELECT 
    F1 AS SKU,
    * EXCLUDE ("F1")
  
  FROM RLS20211129_001_615 AS in0

),

Filter_609 AS (

  SELECT * 
  
  FROM AlteryxSelect_616 AS in0
  
  WHERE (CONTAINS((coalesce(LOWER(SKU), '')), LOWER('Totals for Item')))

),

Formula_608_0 AS (

  SELECT 
    CAST(TRIM((REGEXP_REPLACE(SKU, 'Totals for Item ', ''))) AS STRING) AS SKU,
    CAST('2010' AS STRING) AS SOURCE_WH_DESC,
    CAST((
      coalesce(
        CAST((
          CASE
            WHEN (F11 IS NULL)
              THEN F10
            ELSE F11
          END
        ) AS DOUBLE), 
        (
          REGEXP_SUBSTR((
            CASE
              WHEN (F11 IS NULL)
                THEN F10
              ELSE F11
            END
          ), '^[0-9]+')
        ), 
        0)
    ) AS INTEGER) AS QTY,
    * EXCLUDE ("SKU")
  
  FROM Filter_609 AS in0

),

AlteryxSelect_617 AS (

  SELECT 
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU AS SKU,
    QTY AS QTY,
    FILENAME AS FILENAME
  
  FROM Formula_608_0 AS in0

),

Cleanse_610 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_617'], 
      [
        { "name": "SOURCE_WH_DESC", "dataType": "String" }, 
        { "name": "SKU", "dataType": "String" }, 
        { "name": "QTY", "dataType": "Number" }, 
        { "name": "FILENAME", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['QTY'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      true, 
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

AlteryxSelect_611 AS (

  SELECT 
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU AS SKU,
    CAST(QTY AS INTEGER) AS QTY,
    FILENAME AS FILENAME
  
  FROM Cleanse_610 AS in0

),

Summarize_613 AS (

  SELECT 
    SUM(QTY) AS QTY,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU AS SKU,
    FILENAME AS FILENAME
  
  FROM AlteryxSelect_611 AS in0
  
  GROUP BY 
    SOURCE_WH_DESC, SKU, FILENAME

),

DbFileInput_254_254 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'DbFileInput_254_254') }}

),

Filter_245 AS (

  SELECT * 
  
  FROM DbFileInput_254_254 AS in0
  
  WHERE (NOT(F3 IS NULL))

),

DynamicRename_252 AS (

  SELECT 
    "INVENTORY LIST" AS "PRODUCT CODE",
    F2 AS DESCRIPTION,
    F3 AS "ONHAND QTY",
    F4 AS "PENDING QTY",
    F5 AS "NA QTY",
    F6 AS "HOLD QTY",
    F7 AS "AVAIL QTY",
    F8 AS FIELD_10,
    FILENAME AS "12__13__21_IWI"
  
  FROM Filter_245 AS in0

),

DynamicRename_252_row_number AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_252'], 
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

DynamicRename_252_filter AS (

  SELECT * 
  
  FROM DynamicRename_252_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR (PROPHECY_ROW_ID IS NULL)
        )

),

DynamicRename_252_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_252_filter AS in0

),

Formula_247_0 AS (

  SELECT 
    CAST('IWI Franklin' AS STRING) AS SOURCE_WH_DESC,
    CAST('1_iv Franklin Inventory' AS STRING) AS FILENAME,
    *
  
  FROM DynamicRename_252_drop_0 AS in0

),

AlteryxSelect_246 AS (

  SELECT 
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    DESCRIPTION AS SKU,
    CAST("ONHAND QTY" AS INTEGER) AS QTY,
    FILENAME AS FILENAME
  
  FROM Formula_247_0 AS in0

),

TextToColumns_248 AS (

  {{
    prophecy_basics.TextToColumns(
      ['AlteryxSelect_246'], 
      'SKU', 
      "\\\ ", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'SKU', 
      'SKU', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_248_dropGem_0 AS (

  SELECT 
    SKU_1_SKU AS SKU1,
    SKU_2_SKU AS SKU2,
    * EXCLUDE ("SKU_1_SKU", "SKU_2_SKU")
  
  FROM TextToColumns_248 AS in0

),

Formula_249_0 AS (

  SELECT 
    (
      CASE
        WHEN (SKU1 = '3')
          THEN 'RGF3CP'
        WHEN (SKU1 = 'RGF5CHIKENC')
          THEN 'RGF5CHICENC'
        ELSE SKU1
      END
    ) AS SKU,
    * EXCLUDE ("SKU")
  
  FROM TextToColumns_248_dropGem_0 AS in0

),

AlteryxSelect_250 AS (

  SELECT 
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU AS SKU,
    QTY AS QTY,
    FILENAME AS FILENAME
  
  FROM Formula_249_0 AS in0

),

DbFileInput_35_35 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'DbFileInput_35_35') }}

),

Filter_44 AS (

  SELECT * 
  
  FROM DbFileInput_35_35 AS in0
  
  WHERE (STATUS = 'On Hand')

),

AlteryxSelect_37 AS (

  SELECT 
    SCHEMA_DESC AS SOURCE_WH_DESC,
    PRODUCT AS SKU,
    QTY AS QTY,
    FILENAME AS FILENAME
  
  FROM Filter_44 AS in0

),

DbFileInput_255_255 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'DbFileInput_255_255') }}

),

Formula_257_0 AS (

  SELECT 
    CAST('City of Industry' AS STRING) AS SOURCE_WH_DESC,
    *
  
  FROM DbFileInput_255_255 AS in0

),

AlteryxSelect_256 AS (

  SELECT 
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    F2 AS SKU,
    FILENAME AS FILENAME,
    F6 AS QTY
  
  FROM Formula_257_0 AS in0

),

DbFileInput_572_572 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'DbFileInput_572_572') }}

),

AlteryxSelect_573 AS (

  SELECT 
    F2 AS SKU,
    F6 AS QTY,
    FILENAME AS FILENAME
  
  FROM DbFileInput_572_572 AS in0

),

Filter_575 AS (

  SELECT * 
  
  FROM AlteryxSelect_573 AS in0
  
  WHERE (
          NOT(
            (LENGTH(SKU)) = 0)
        )

),

Formula_574_0 AS (

  SELECT 
    CAST('2009' AS STRING) AS SOURCE_WH_DESC,
    *
  
  FROM Filter_575 AS in0

),

Cleanse_576 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_574_0'], 
      [
        { "name": "SOURCE_WH_DESC", "dataType": "String" }, 
        { "name": "SKU", "dataType": "String" }, 
        { "name": "QTY", "dataType": "Float" }, 
        { "name": "FILENAME", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['QTY'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      true, 
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

AlteryxSelect_577 AS (

  SELECT 
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU AS SKU,
    CAST(QTY AS INTEGER) AS QTY,
    FILENAME AS FILENAME
  
  FROM Cleanse_576 AS in0

),

Summarize_579 AS (

  SELECT 
    SUM(QTY) AS QTY,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SKU AS SKU,
    FILENAME AS FILENAME
  
  FROM AlteryxSelect_577 AS in0
  
  GROUP BY 
    SOURCE_WH_DESC, SKU, FILENAME

),

Union_160 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'AlteryxSelect_256', 
        'Summarize_613', 
        'Summarize_523', 
        'AlteryxSelect_250', 
        'Summarize_579', 
        'AlteryxSelect_128', 
        'Summarize_580', 
        'AlteryxSelect_37'
      ], 
      [
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "QTY", "dataType": "Number"}]', 
        '[{"name": "QTY", "dataType": "Number"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}]', 
        '[{"name": "QTY", "dataType": "Number"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "QTY", "dataType": "Number"}, {"name": "FILENAME", "dataType": "String"}]', 
        '[{"name": "QTY", "dataType": "Number"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "QTY", "dataType": "Number"}, {"name": "FILENAME", "dataType": "String"}]', 
        '[{"name": "QTY", "dataType": "Number"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "QTY", "dataType": "Number"}, {"name": "FILENAME", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

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
        { "name": "ROWTYPE", "dataType": "String" }, 
        { "name": "ROWSORTTIER", "dataType": "Number" }, 
        { "name": "SOURCE_WH_DESC", "dataType": "String" }, 
        { "name": "SKU", "dataType": "String" }, 
        { "name": "FILENAME", "dataType": "String" }, 
        { "name": "QTY", "dataType": "Number" }
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
  
  FROM {{ prophecy_tmp_source('p1', 'DbFileInput_297_297') }}

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
    CAST((date_part('epoch_second', (TO_TIMESTAMP((REGEXP_REPLACE(VARIABLEDATE, '\\.\\d+', '')), 'MM/DD/YY')))) AS STRING) AS VARIABLEDATE,
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
  
  FROM {{ ref('p1__Formula_282_0')}}

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
    CAST((
      date_part(
        'epoch_second', 
        (TO_TIMESTAMP((REGEXP_REPLACE("CLIENTSLASHVENDOR MESSAGE3", '\\.\\d+', '')), 'MMDDYY')))
    ) AS STRING) AS VARIABLEDATE,
    CAST("CLIENTSLASHVENDOR MESSAGE2" AS STRING) AS SOURCE_WH_DESC,
    (-1 * QTY) AS QTY,
    * EXCLUDE ("VARIABLEDATE", "SOURCE_WH_DESC", "QTY")
  
  FROM TextToColumns_498_dropGem_0 AS in0

),

Formula_499_1 AS (

  SELECT 
    (TO_CHAR(VARIABLEDATE)) AS SHIP_DT,
    (
      TO_CHAR(
        (
          date_part(
            'epoch_second', 
            (TO_TIMESTAMP((REGEXP_REPLACE("CLIENTSLASHVENDOR MESSAGE6", '\\.\\d+', '')), 'MMDDYY')))
        ), 
        'YYYY-MM-DD')
    ) AS DELIVERY_DT,
    *
  
  FROM Formula_499_0 AS in0

),

Formula_307_0 AS (

  SELECT 
    CAST("CLIENTSLASHVENDOR MESSAGE5" AS STRING) AS SOURCE_WH_DESC,
    (-1 * QTY) AS QTY,
    CAST((
      date_part(
        'epoch_second', 
        (TO_TIMESTAMP((REGEXP_REPLACE("CLIENTSLASHVENDOR MESSAGE6", '\\.\\d+', '')), 'MMDDYY')))
    ) AS STRING) AS VARIABLEDATE,
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
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "QTY", "dataType": "Number"}, {"name": "VARIABLEDATE", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE1", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE2", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE3", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE4", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE5", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE6", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "FROM_WH_ID", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Number"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER_FLAG", "dataType": "String"}, {"name": "RATE", "dataType": "String"}, {"name": "TOTAL AMT", "dataType": "String"}, {"name": "CREATED BY", "dataType": "String"}, {"name": "PRODUCTSLASHSERVICE", "dataType": "String"}, {"name": "ACCOUNT", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "TAX NAME", "dataType": "String"}, {"name": "LAST MODIFIED BY", "dataType": "String"}, {"name": "RECEIVED AMT", "dataType": "String"}, {"name": "VENDOR", "dataType": "String"}, {"name": "RECEIVED QTY", "dataType": "String"}, {"name": "LAST MODIFIED", "dataType": "String"}, {"name": "TAXABLE AMOUNT", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "OPEN BALANCE", "dataType": "String"}, {"name": "TAX AMOUNT", "dataType": "String"}, {"name": "BACKORDERED QTY", "dataType": "String"}, {"name": "CLASS", "dataType": "String"}, {"name": "MEMOSLASHDESCRIPTION", "dataType": "String"}, {"name": "FIELD_26", "dataType": "String"}, {"name": "CREATE DATE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_1_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_2_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_3_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_4_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_5_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_6_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "FROM_WH_ID", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Number"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "QTY", "dataType": "Number"}, {"name": "PRODUCTION_OR_TRANSFER_FLAG", "dataType": "String"}, {"name": "VARIABLEDATE", "dataType": "String"}, {"name": "RATE", "dataType": "String"}, {"name": "TOTAL AMT", "dataType": "String"}, {"name": "CREATED BY", "dataType": "String"}, {"name": "PRODUCTSLASHSERVICE", "dataType": "String"}, {"name": "ACCOUNT", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "TAX NAME", "dataType": "String"}, {"name": "LAST MODIFIED BY", "dataType": "String"}, {"name": "RECEIVED AMT", "dataType": "String"}, {"name": "VENDOR", "dataType": "String"}, {"name": "RECEIVED QTY", "dataType": "String"}, {"name": "LAST MODIFIED", "dataType": "String"}, {"name": "TAXABLE AMOUNT", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "OPEN BALANCE", "dataType": "String"}, {"name": "TAX AMOUNT", "dataType": "String"}, {"name": "BACKORDERED QTY", "dataType": "String"}, {"name": "CLASS", "dataType": "String"}, {"name": "MEMOSLASHDESCRIPTION", "dataType": "String"}, {"name": "FIELD_26", "dataType": "String"}, {"name": "CREATE DATE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}]', 
        '[{"name": "SHIP_DT", "dataType": "String"}, {"name": "DELIVERY_DT", "dataType": "String"}, {"name": "VARIABLEDATE", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "QTY", "dataType": "Number"}, {"name": "CLIENTSLASHVENDOR MESSAGE1", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE2", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE3", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE4", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE5", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE6", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "FROM_WH_ID", "dataType": "String"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "ROWSORTTIER", "dataType": "Number"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER_FLAG", "dataType": "String"}, {"name": "RATE", "dataType": "String"}, {"name": "TOTAL AMT", "dataType": "String"}, {"name": "CREATED BY", "dataType": "String"}, {"name": "PRODUCTSLASHSERVICE", "dataType": "String"}, {"name": "ACCOUNT", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "TAX NAME", "dataType": "String"}, {"name": "LAST MODIFIED BY", "dataType": "String"}, {"name": "RECEIVED AMT", "dataType": "String"}, {"name": "VENDOR", "dataType": "String"}, {"name": "RECEIVED QTY", "dataType": "String"}, {"name": "LAST MODIFIED", "dataType": "String"}, {"name": "TAXABLE AMOUNT", "dataType": "String"}, {"name": "CUSTOMER", "dataType": "String"}, {"name": "OPEN BALANCE", "dataType": "String"}, {"name": "TAX AMOUNT", "dataType": "String"}, {"name": "BACKORDERED QTY", "dataType": "String"}, {"name": "CLASS", "dataType": "String"}, {"name": "MEMOSLASHDESCRIPTION", "dataType": "String"}, {"name": "FIELD_26", "dataType": "String"}, {"name": "CREATE DATE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_1_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_2_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_3_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_4_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_5_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}, {"name": "CLIENTSLASHVENDOR MESSAGE_6_CLIENTSLASHVENDOR MESSAGE", "dataType": "String"}]'
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
    (TO_CHAR(DELIVERY_DT)) AS DELIVERY_DT,
    FILENAME AS FILENAME,
    CAST(PO_NUMBER AS STRING) AS PO_NUMBER,
    CAST(PRODUCTION_OR_TRANSFER AS STRING) AS PRODUCTION_OR_TRANSFER,
    QTY AS QTY,
    ROWSORTTIER AS ROWSORTTIER,
    ROWTYPE AS ROWTYPE,
    (TO_CHAR(SHIP_DT)) AS SHIP_DT,
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

Union_191_reformat_2 AS (

  SELECT 
    CAST(CUSTOMER AS STRING) AS CUSTOMER,
    DATE_OF_EVENT AS DATE_OF_EVENT,
    (TO_CHAR(DELIVERY_DT)) AS DELIVERY_DT,
    FILENAME AS FILENAME,
    CAST(PICKUP_OR_DELIVERY AS STRING) AS PICKUP_OR_DELIVERY,
    CAST(PO_NUMBER AS STRING) AS PO_NUMBER,
    CAST(QTY AS DOUBLE) AS QTY,
    ROWSORTTIER AS ROWSORTTIER,
    ROWTYPE AS ROWTYPE,
    CAST(SALES_ORDER AS STRING) AS SALES_ORDER,
    (TO_CHAR(SHIP_DT)) AS SHIP_DT,
    SKU AS SKU,
    SOURCE_WH_DESC AS SOURCE_WH_DESC
  
  FROM Formula_282_0 AS in0

),

Union_191 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_191_reformat_0', 'Union_191_reformat_2', 'Union_191_reformat_1'], 
      [
        '[{"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "FILENAME", "dataType": "String"}, {"name": "QTY", "dataType": "Float"}, {"name": "ROWSORTTIER", "dataType": "Number"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}]', 
        '[{"name": "CUSTOMER", "dataType": "String"}, {"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "DELIVERY_DT", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "PICKUP_OR_DELIVERY", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "QTY", "dataType": "Float"}, {"name": "ROWSORTTIER", "dataType": "Number"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SALES_ORDER", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}]', 
        '[{"name": "DATE_OF_EVENT", "dataType": "Date"}, {"name": "DELIVERY_DT", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "PO_NUMBER", "dataType": "String"}, {"name": "PRODUCTION_OR_TRANSFER", "dataType": "String"}, {"name": "QTY", "dataType": "Number"}, {"name": "ROWSORTTIER", "dataType": "Number"}, {"name": "ROWTYPE", "dataType": "String"}, {"name": "SHIP_DT", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "SOURCE_WH_DESC", "dataType": "String"}]'
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
        { "name": "SOURCE_SKU", "dataType": "String" }, 
        { "name": "DATE_OF_EVENT", "dataType": "Date" }, 
        { "name": "FILENAME", "dataType": "String" }, 
        { "name": "QTY", "dataType": "Float" }, 
        { "name": "ROWSORTTIER", "dataType": "Number" }, 
        { "name": "ROWTYPE", "dataType": "String" }, 
        { "name": "SOURCE_WH_DESC", "dataType": "String" }, 
        { "name": "CUSTOMER", "dataType": "String" }, 
        { "name": "DELIVERY_DT", "dataType": "String" }, 
        { "name": "PICKUP_OR_DELIVERY", "dataType": "String" }, 
        { "name": "PO_NUMBER", "dataType": "String" }, 
        { "name": "SALES_ORDER", "dataType": "String" }, 
        { "name": "SHIP_DT", "dataType": "String" }, 
        { "name": "PRODUCTION_OR_TRANSFER", "dataType": "String" }
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
