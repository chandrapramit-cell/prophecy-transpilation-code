{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_533 AS (

  SELECT *
  
  FROM {{ ref('p1__AlteryxSelect_533')}}

),

AlteryxSelect_557 AS (

  SELECT * EXCLUDE ("FILENAME")
  
  FROM AlteryxSelect_533 AS in0

),

SelectRecords_535_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['AlteryxSelect_557'], 
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

SelectRecords_535 AS (

  SELECT * 
  
  FROM SelectRecords_535_rowNumber AS in0
  
  WHERE (ROW_NUMBER >= 3)

),

SelectRecords_535_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_535 AS in0

),

DynamicRename_538 AS (

  SELECT 
    F1 AS "SKU HASH",
    F8 AS PLANT,
    F9 AS LINE,
    F10 AS ACTIVE,
    F11 AS "PERCENT ALLOCATION",
    FORECAST AS "2021-07-01",
    FORECAST2 AS "2021-08-01",
    FORECAST3 AS "2021-09-01",
    FORECAST4 AS "2021-10-01",
    FORECAST5 AS "2021-11-01",
    FORECAST6 AS "2021-12-01",
    FORECAST7 AS "2022-01-01",
    FORECAST8 AS "2022-02-01",
    FORECAST9 AS "2022-03-01",
    FORECAST9_2 AS "2022-04-01",
    FORECAST9_3 AS "2022-05-01",
    FORECAST9_4 AS "2022-06-01",
    FORECAST9_5 AS "2022-07-01",
    FORECAST9_6 AS "2022-08-01",
    FORECAST9_7 AS "2022-09-01",
    FORECAST9_8 AS "2022-10-01",
    FORECAST9_9 AS "2022-11-01",
    FORECAST9_10 AS "2022-12-01",
    F30 AS FIELD_52,
    F31 AS FIELD_52_2,
    F32 AS FIELD_52_3,
    F33 AS FIELD_52_4,
    F34 AS FIELD_52_5,
    F35 AS FIELD_52_6,
    F36 AS FIELD_52_7,
    F37 AS FIELD_52_8,
    F38 AS FIELD_52_9,
    F39 AS FIELD_52_10,
    F40 AS FIELD_52_11,
    F41 AS FIELD_52_12,
    F42 AS FIELD_52_13,
    F43 AS FIELD_52_14,
    F44 AS FIELD_52_15,
    F45 AS FIELD_52_16,
    F46 AS FIELD_52_17,
    F47 AS FIELD_52_18,
    F48 AS FIELD_52_19,
    F49 AS FIELD_52_20,
    F50 AS FIELD_52_21,
    F51 AS FIELD_52_22,
    F52 AS FIELD_52_23,
    F53 AS FIELD_52_24,
    F54 AS FIELD_52_25,
    F55 AS FIELD_52_26,
    F56 AS FIELD_52_27,
    F57 AS FIELD_52_28
  
  FROM SelectRecords_535_cleanup_0 AS in0

),

DynamicRename_538_row_number AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_538'], 
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

DynamicRename_538_filter AS (

  SELECT * 
  
  FROM DynamicRename_538_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR (PROPHECY_ROW_ID IS NULL)
        )

),

DynamicRename_538_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_538_filter AS in0

),

DynamicSelect_536 AS (

  SELECT 
    "2022-02-01" AS "2022-02-01",
    "2022-04-01" AS "2022-04-01",
    "2022-03-01" AS "2022-03-01",
    "2022-06-01" AS "2022-06-01",
    "SKU HASH" AS "SKU HASH",
    "2022-01-01" AS "2022-01-01",
    "2022-05-01" AS "2022-05-01"
  
  FROM DynamicRename_538_drop_0 AS in0

),

Cleanse_543 AS (

  {{
    prophecy_basics.DataCleansing(
      ['DynamicSelect_536'], 
      [
        { "name": "2022-02-01", "dataType": "String" }, 
        { "name": "2022-04-01", "dataType": "String" }, 
        { "name": "2022-03-01", "dataType": "String" }, 
        { "name": "2022-06-01", "dataType": "String" }, 
        { "name": "SKU HASH", "dataType": "String" }, 
        { "name": "2022-01-01", "dataType": "String" }, 
        { "name": "2022-05-01", "dataType": "String" }
      ], 
      'makeUppercase', 
      [], 
      true, 
      '', 
      true, 
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

),

MultiFieldFormula_544 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['Cleanse_543'], 
      "column_name", 
      ['2022-02-01', '2022-04-01', '2022-03-01', '2022-06-01', 'SKU HASH', '2022-01-01', '2022-05-01'], 
      ['2022-02-01', '2022-04-01'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Filter_545 AS (

  SELECT * 
  
  FROM MultiFieldFormula_544 AS in0
  
  WHERE (
          (NOT(("SKU HASH" IS NULL) OR ((LENGTH("SKU HASH")) = 0)))
          AND (
                (
                  NOT(
                    "SKU HASH" = 'Total Cases')
                ) OR ("SKU HASH" IS NULL)
              )
        )

),

AlteryxSelect_547 AS (

  SELECT "SKU HASH" AS SKU
  
  FROM Filter_545 AS in0

)

SELECT *

FROM AlteryxSelect_547
