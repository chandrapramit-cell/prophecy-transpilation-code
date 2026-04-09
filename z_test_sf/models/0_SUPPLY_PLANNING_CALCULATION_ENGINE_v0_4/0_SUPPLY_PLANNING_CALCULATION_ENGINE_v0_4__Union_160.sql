{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DbFileInput_503_503 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'DbFileInput_503_503') }}

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

AppendFields_515 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM SelectRecords_514_cleanup_0 AS in0
  INNER JOIN Cleanse_511 AS in1
     ON TRUE

),

RLS20211129_001_615 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'RLS20211129_001_615') }}

),

AlteryxSelect_616 AS (

  SELECT 
    F1 AS SKU,
    * EXCLUDE ("F1")
  
  FROM RLS20211129_001_615 AS in0

),

DbFileInput_254_254 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'DbFileInput_254_254') }}

),

DbFileInput_255_255 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'DbFileInput_255_255') }}

),

Lineage11_01_21_570 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'Lineage11_01_21_570') }}

),

AlteryxSelect_519 AS (

  SELECT 
    "DESCRIPTION SLASH INVENTORY DETAILS" AS SKU,
    FIELD_18 AS QTY,
    FILENAME AS FILENAME
  
  FROM Lineage11_01_21_570 AS in0

),

DbFileInput_127_127 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'DbFileInput_127_127') }}

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

AlteryxSelect_128 AS (

  SELECT 
    "FACILITY NAME" AS SOURCE_WH_DESC,
    "ITEM CODE" AS SKU,
    "ON HAND" AS QTY,
    FILENAME AS FILENAME
  
  FROM DbFileInput_127_127 AS in0

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
        { "name": "SKU", "dataType": "String" }, 
        { "name": "QTY", "dataType": "Double" }, 
        { "name": "FILENAME", "dataType": "String" }, 
        { "name": "SOURCE_WH_DESC", "dataType": "String" }
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

DbFileInput_35_35 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'DbFileInput_35_35') }}

),

Filter_44 AS (

  SELECT * 
  
  FROM DbFileInput_35_35 AS in0
  
  WHERE (STATUS = 'On Hand')

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
        { "name": "QTY", "dataType": "Double" }, 
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

AlteryxSelect_37 AS (

  SELECT 
    SCHEMA_DESC AS SOURCE_WH_DESC,
    PRODUCT AS SKU,
    QTY AS QTY,
    FILENAME AS FILENAME
  
  FROM Filter_44 AS in0

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
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'DbFileInput_572_572') }}

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
        { "name": "SKU", "dataType": "String" }, 
        { "name": "QTY", "dataType": "Double" }, 
        { "name": "FILENAME", "dataType": "String" }, 
        { "name": "SOURCE_WH_DESC", "dataType": "String" }
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
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "QTY", "dataType": "Double"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "QTY", "dataType": "Double"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "QTY", "dataType": "Double"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "QTY", "dataType": "Double"}, {"name": "FILENAME", "dataType": "String"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "QTY", "dataType": "Double"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "QTY", "dataType": "Double"}, {"name": "FILENAME", "dataType": "String"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "FILENAME", "dataType": "String"}, {"name": "QTY", "dataType": "Double"}]', 
        '[{"name": "SOURCE_WH_DESC", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "QTY", "dataType": "Double"}, {"name": "FILENAME", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_160
