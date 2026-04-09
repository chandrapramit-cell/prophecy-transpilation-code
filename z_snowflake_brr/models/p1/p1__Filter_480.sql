{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH WOH120921_xlsx__476 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('p1', 'WOH120921_xlsx__476') }}

),

Filter_481 AS (

  SELECT * 
  
  FROM WOH120921_xlsx__476 AS in0
  
  WHERE not((3 IS NULL))

),

DynamicRename_478 AS (

  SELECT 
    1 AS SKU,
    2 AS VARIABLEDESC,
    3 AS "PRP2 DESC",
    4 AS "PRP3 DESC",
    5 AS F21WK36,
    6 AS F21WK37,
    7 AS F21WK38,
    8 AS F21WK39,
    9 AS F21WK40,
    10 AS F21WK41,
    11 AS F21WK42,
    12 AS F21WK43,
    13 AS F21WK44,
    14 AS F21WK45,
    15 AS F21WK46,
    16 AS F21WK47,
    17 AS F21WK48,
    18 AS F21WK49,
    F19 AS TOTAL,
    19 AS "AVERAGE 14 WEEKS",
    20 AS "AVERAGE LAST 6 WEEKS"
  
  FROM Filter_481 AS in0

),

DynamicRename_478_row_number AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_478'], 
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

DynamicRename_478_filter AS (

  SELECT * 
  
  FROM DynamicRename_478_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR (PROPHECY_ROW_ID IS NULL)
        )

),

DynamicRename_478_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_478_filter AS in0

),

AlteryxSelect_477 AS (

  SELECT 
    SKU AS SKU,
    "AVERAGE 14 WEEKS" AS "13_WK_WKLY_AVG_SALES"
  
  FROM DynamicRename_478_drop_0 AS in0

),

Formula_479_0 AS (

  SELECT 
    1 AS "13_WK_WKLY_AVG_SALES",
    * EXCLUDE ("13_WK_WKLY_AVG_SALES")
  
  FROM AlteryxSelect_477 AS in0

),

AlteryxSelect_486 AS (

  SELECT 
    CAST("13_WK_WKLY_AVG_SALES" AS INTEGER) AS "13_WK_WKLY_AVG_SALES",
    * EXCLUDE ("13_WK_WKLY_AVG_SALES")
  
  FROM Formula_479_0 AS in0

),

Cleanse_482 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_486'], 
      [{ "name": "13_WK_WKLY_AVG_SALES", "dataType": "Number" }, { "name": "SKU", "dataType": "String" }], 
      'makeUppercase', 
      ['SKU'], 
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

),

Filter_480 AS (

  SELECT * 
  
  FROM Cleanse_482 AS in0
  
  WHERE (
          (NOT(SKU IS NULL))
          AND (
                (
                  NOT(
                    SKU = 'Total')
                ) OR (SKU IS NULL)
              )
        )

)

SELECT *

FROM Filter_480
