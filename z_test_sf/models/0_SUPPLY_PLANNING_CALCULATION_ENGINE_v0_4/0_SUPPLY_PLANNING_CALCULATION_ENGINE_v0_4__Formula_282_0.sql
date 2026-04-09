{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH SalesQB11_10_21_264 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'SalesQB11_10_21_264') }}

),

Filter_270 AS (

  SELECT * 
  
  FROM SalesQB11_10_21_264 AS in0
  
  WHERE ((NOT(F2 IS NULL)) AND (NOT(F5 IS NULL)))

),

DynamicRename_269 AS (

  SELECT 
    "REAL GOOD FOOD COMPANY" AS FIELD_12,
    F2 AS VARIABLEDATE,
    F3 AS "TRANSACTION TYPE",
    F4 AS NUM,
    F5 AS "PO NUMBER",
    F6 AS CUSTOMER,
    F7 AS PRODUCTSLASHSERVICE,
    F8 AS SKU,
    F9 AS QTY,
    F10 AS "SALES PRICE",
    F11 AS AMOUNT
  
  FROM Filter_270 AS in0

),

DynamicRename_269_row_number AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_269'], 
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

DynamicRename_269_filter AS (

  SELECT * 
  
  FROM DynamicRename_269_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR (PROPHECY_ROW_ID IS NULL)
        )

),

DynamicRename_269_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_269_filter AS in0

),

DbFileInput_265_265 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'DbFileInput_265_265') }}

),

Filter_267 AS (

  SELECT * 
  
  FROM DbFileInput_265_265 AS in0
  
  WHERE (NOT(F2 IS NULL))

),

DynamicRename_266 AS (

  SELECT 
    F1 AS FIELD_8,
    F2 AS VARIABLEDATE,
    F3 AS "EXPIRATION DATE",
    F4 AS NAME,
    F5 AS "PO NUMBER",
    F6 AS AMOUNT,
    F7 AS MEMOSLASHDESCRIPTION
  
  FROM Filter_267 AS in0

),

DynamicRename_266_row_number AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_266'], 
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

DynamicRename_266_filter AS (

  SELECT * 
  
  FROM DynamicRename_266_row_number AS in0
  
  WHERE (
          (
            NOT(
              PROPHECY_ROW_ID = 1)
          ) OR (PROPHECY_ROW_ID IS NULL)
        )

),

DynamicRename_266_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM DynamicRename_266_filter AS in0

),

TextToColumns_273 AS (

  {{
    prophecy_basics.TextToColumns(
      ['DynamicRename_266_drop_0'], 
      'MEMOSLASHDESCRIPTION', 
      ",", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'MEMOSLASHDESCRIPTION', 
      'MEMOSLASHDESCRIPTION', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

Formula_272_0 AS (

  SELECT 
    CAST((datepart('' epoch_second '', (TO_TIMESTAMP((REGEXP_REPLACE(VARIABLEDATE, '\\.\\d+', '')), 'MM/DD/YY')))) AS STRING) AS VARIABLEDATE,
    CAST((
      (
        CASE
          WHEN (
            (((coalesce(CAST(QTY AS DOUBLE), CAST((REGEXP_SUBSTR(QTY, '^[0-9]+')) AS INTEGER), 0)) / 1) < 0)
            AND (
                  (
                    ((coalesce(CAST(QTY AS DOUBLE), CAST((REGEXP_SUBSTR(QTY, '^[0-9]+')) AS INTEGER), 0)) / 1)
                    - FLOOR(((coalesce(CAST(QTY AS DOUBLE), CAST((REGEXP_SUBSTR(QTY, '^[0-9]+')) AS INTEGER), 0)) / 1))
                  ) = 0.5
                )
          )
            THEN CEIL(((coalesce(CAST(QTY AS DOUBLE), CAST((REGEXP_SUBSTR(QTY, '^[0-9]+')) AS INTEGER), 0)) / 1))
          ELSE ROUND(((coalesce(CAST(QTY AS DOUBLE), CAST((REGEXP_SUBSTR(QTY, '^[0-9]+')) AS INTEGER), 0)) / 1))
        END
      )
      * 1
    ) AS STRING) AS QTY,
    * EXCLUDE ("VARIABLEDATE", "QTY")
  
  FROM DynamicRename_269_drop_0 AS in0

),

Filter_280 AS (

  SELECT * 
  
  FROM Formula_272_0 AS in0
  
  WHERE (
          (
            (
              (NOT(SKU IS NULL)) AND (
                    (
                      NOT(
                        QTY = '0')
                    ) OR (QTY IS NULL)
                  )
            )
            AND (NOT(CONTAINS((coalesce(LOWER(UPPER(SKU)), '')), LOWER('OFF INVOICE'))))
          )
          AND (NOT(CONTAINS((coalesce(LOWER(UPPER(SKU)), '')), LOWER('CREDIT'))))
        )

),

TextToColumns_273_dropGem_0 AS (

  SELECT 
    MEMOSLASHDESCRIPTION_1_MEMOSLASHDESCRIPTION AS MEMOSLASHDESCRIPTION1,
    MEMOSLASHDESCRIPTION_2_MEMOSLASHDESCRIPTION AS MEMOSLASHDESCRIPTION2,
    *
  
  FROM TextToColumns_273 AS in0

),

Formula_274_0 AS (

  SELECT 
    CAST((datepart('' epoch_second '', (TO_TIMESTAMP((REGEXP_REPLACE(VARIABLEDATE, '\\.\\d+', '')), 'MM/DD/YY')))) AS STRING) AS VARIABLEDATE,
    * EXCLUDE ("VARIABLEDATE")
  
  FROM TextToColumns_273_dropGem_0 AS in0

),

Formula_274_1 AS (

  SELECT 
    (TO_CHAR(VARIABLEDATE, 'YYYY-MM-DD')) AS SHIP_DT,
    to_char(
      date_part('EPOCH_SECOND', to_timestamp(regexp_replace("" EXPIRATION DATE"", '\\.\\d+', ''), 'MM/DD/YY')), 
      'YYYY-MM-DD') AS DELIVERY_DT,
    *
  
  FROM Formula_274_0 AS in0

),

Summarize_277 AS (

  SELECT 
    MIN(VARIABLEDATE) AS DATE_OF_EVENT,
    MAX(AMOUNT) AS AMOUNT,
    MIN(SHIP_DT) AS SHIP_DT,
    MIN(DELIVERY_DT) AS DELIVERY_DT,
    NAME AS NAME,
    MEMOSLASHDESCRIPTION2 AS MEMOSLASHDESCRIPTION2,
    MEMOSLASHDESCRIPTION AS MEMOSLASHDESCRIPTION,
    MEMOSLASHDESCRIPTION1 AS MEMOSLASHDESCRIPTION1,
    "PO NUMBER" AS "PO NUMBER"
  
  FROM Formula_274_1 AS in0
  
  GROUP BY 
    NAME, MEMOSLASHDESCRIPTION2, MEMOSLASHDESCRIPTION, MEMOSLASHDESCRIPTION1, "PO NUMBER"

),

AlteryxSelect_268 AS (

  SELECT 
    DATE_OF_EVENT AS DATE_OF_EVENT,
    NAME AS CUSTOMER,
    "PO NUMBER" AS PO_NUMBER,
    MEMOSLASHDESCRIPTION1 AS PICKUP_OR_DELIVERY,
    MEMOSLASHDESCRIPTION2 AS SOURCE_WH_DESC,
    SHIP_DT AS SHIP_DT,
    DELIVERY_DT AS DELIVERY_DT
  
  FROM Summarize_277 AS in0

),

AlteryxSelect_271 AS (

  SELECT 
    VARIABLEDATE AS VARIABLEDATE,
    NUM AS SALES_ORDER,
    "PO NUMBER" AS PO_NUMBER,
    CUSTOMER AS CUSTOMER,
    SKU AS SKU,
    CAST(QTY AS INTEGER) AS QTY
  
  FROM Filter_280 AS in0

),

Join_275_inner AS (

  SELECT 
    in1.SKU AS SKU,
    in0.PICKUP_OR_DELIVERY AS PICKUP_OR_DELIVERY,
    in0.DATE_OF_EVENT AS DATE_OF_EVENT,
    in0.DELIVERY_DT AS DELIVERY_DT,
    in0.PO_NUMBER AS PO_NUMBER,
    in0.CUSTOMER AS CUSTOMER,
    in1.QTY AS QTY,
    in1.SALES_ORDER AS SALES_ORDER,
    in0.SHIP_DT AS SHIP_DT,
    in0.SOURCE_WH_DESC AS SOURCE_WH_DESC
  
  FROM AlteryxSelect_268 AS in0
  INNER JOIN AlteryxSelect_271 AS in1
     ON ((in0.PO_NUMBER = in1.PO_NUMBER) AND (in0.CUSTOMER = in1.CUSTOMER))

),

AlteryxSelect_279 AS (

  SELECT 
    CAST((TRY_TO_TIMESTAMP(CAST(DATE_OF_EVENT AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS DATE) AS DATE_OF_EVENT,
    CUSTOMER AS CUSTOMER,
    SALES_ORDER AS SALES_ORDER,
    PO_NUMBER AS PO_NUMBER,
    PICKUP_OR_DELIVERY AS PICKUP_OR_DELIVERY,
    SOURCE_WH_DESC AS SOURCE_WH_DESC,
    SHIP_DT AS SHIP_DT,
    DELIVERY_DT AS DELIVERY_DT,
    SKU AS SKU,
    QTY AS QTY
  
  FROM Join_275_inner AS in0

),

Formula_282_0 AS (

  SELECT 
    CAST('2_SALES_BY_CUSTOMER_DETAIL' AS STRING) AS ROWTYPE,
    2 AS ROWSORTTIER,
    CAST('Sales by Customer Detail and Estimates by Customer' AS STRING) AS FILENAME,
    CAST((-1 * QTY) AS INTEGER) AS QTY,
    (
      CASE
        WHEN (SOURCE_WH_DESC IS NULL)
          THEN '5000'
        ELSE SOURCE_WH_DESC
      END
    ) AS SOURCE_WH_DESC,
    * EXCLUDE ("QTY", "SOURCE_WH_DESC")
  
  FROM AlteryxSelect_279 AS in0

)

SELECT *

FROM Formula_282_0
