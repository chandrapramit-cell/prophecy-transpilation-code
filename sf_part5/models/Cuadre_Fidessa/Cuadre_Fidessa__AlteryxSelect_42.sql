{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH FICHEROHYDRA_xl_3 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Cuadre_Fidessa', 'FICHEROHYDRA_xl_3') }}

),

TextToColumns_7 AS (

  {{
    prophecy_basics.TextToColumns(
      ['FICHEROHYDRA_xl_3'], 
      'STOCK', 
      "\-", 
      'splitColumns', 
      3, 
      'leaveExtraCharLastCol', 
      'STOCK', 
      'STOCK', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_7_dropGem_0 AS (

  SELECT 
    STOCK_1_STOCK AS STOCK1,
    STOCK_2_STOCK AS STOCK2,
    STOCK_3_STOCK AS STOCK3,
    *
  
  FROM TextToColumns_7 AS in0

),

Cleanse_9 AS (

  {{
    prophecy_basics.DataCleansing(
      ['TextToColumns_7_dropGem_0'], 
      [
        { "name": "STOCK1", "dataType": "String" }, 
        { "name": "STOCK2", "dataType": "String" }, 
        { "name": "STOCK3", "dataType": "String" }, 
        { "name": "COMMISSION", "dataType": "Float" }, 
        { "name": "BSLASHS", "dataType": "String" }, 
        { "name": "SETTLEMENT", "dataType": "Date" }, 
        { "name": "REF__", "dataType": "String" }, 
        { "name": "QUANTITY", "dataType": "Float" }, 
        { "name": "TRADE DATE", "dataType": "Date" }, 
        { "name": "GROSS_PR__", "dataType": "Float" }, 
        { "name": "BROKER", "dataType": "String" }, 
        { "name": "NET CONSIDERATI", "dataType": "Float" }, 
        { "name": "NET_PR__", "dataType": "Float" }, 
        { "name": "GROSS CONSIDERA", "dataType": "Float" }, 
        { "name": "STOCK", "dataType": "String" }, 
        { "name": "B/S", "dataType": "String" }, 
        { "name": "STOCK_1_STOCK", "dataType": "String" }, 
        { "name": "STOCK_2_STOCK", "dataType": "String" }, 
        { "name": "STOCK_3_STOCK", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['STOCK2'], 
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

AlteryxSelect_11 AS (

  SELECT 
    "NET CONSIDERATI" AS "NET HYDRA",
    QUANTITY AS "QTY HYDRA",
    * EXCLUDE ("REF__", 
    "TRADE DATE", 
    "SETTLEMENT", 
    "BROKER", 
    "STOCK", 
    "GROSS_PR__", 
    "GROSS CONSIDERA", 
    "NET_PR__", 
    "COMMISSION", 
    "STOCK1", 
    "STOCK3", 
    "NET CONSIDERATI", 
    "QUANTITY")
  
  FROM Cleanse_9 AS in0

),

CrossTab_20 AS (

  SELECT STOCK2 AS Stock2
  
  FROM AlteryxSelect_11 AS in0
  PIVOT (
    SUM("QTY HYDRA")
    FOR "B/S"
    IN (
      'B', 'S'
    )
  )

),

CrossTab_20_rename AS (

  SELECT 
    SUM_B AS B,
    SUM_S AS S,
    * EXCLUDE ("SUM_B", "SUM_S")
  
  FROM CrossTab_20 AS in0

),

AlteryxSelect_24 AS (

  SELECT 
    B AS "QTY HYDRA B",
    S AS "QTY HYDRA S",
    * EXCLUDE ("B", "S")
  
  FROM CrossTab_20_rename AS in0

),

CrossTab_21 AS (

  SELECT STOCK2 AS Stock2
  
  FROM AlteryxSelect_11 AS in0
  PIVOT (
    SUM("NET HYDRA")
    FOR "B/S"
    IN (
      'B', 'S'
    )
  )

),

CrossTab_21_rename AS (

  SELECT 
    SUM_B AS B,
    SUM_S AS S,
    * EXCLUDE ("SUM_B", "SUM_S")
  
  FROM CrossTab_21 AS in0

),

AlteryxSelect_25 AS (

  SELECT 
    B AS "NET HYDRA B",
    S AS "NET HYDRA S",
    * EXCLUDE ("B", "S")
  
  FROM CrossTab_21_rename AS in0

),

Join_31_inner AS (

  SELECT 
    in1.STOCK2 AS RIGHT_STOCK2,
    in0.*,
    in1.* EXCLUDE ("STOCK2")
  
  FROM AlteryxSelect_24 AS in0
  INNER JOIN AlteryxSelect_25 AS in1
     ON (in0.STOCK2 = in1.STOCK2)

),

AlteryxSelect_35 AS (

  SELECT * EXCLUDE ("RIGHT_STOCK2")
  
  FROM Join_31_inner AS in0

),

Cleanse_38 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_35'], 
      [
        { "name": "NET HYDRA S", "dataType": "Double" }, 
        { "name": "NET HYDRA B", "dataType": "Double" }, 
        { "name": "QTY HYDRA S", "dataType": "Double" }, 
        { "name": "STOCK2", "dataType": "String" }, 
        { "name": "QTY HYDRA B", "dataType": "Double" }
      ], 
      'keepOriginal', 
      ['QTY HYDRA B', 'QTY HYDRA S', 'NET HYDRA B', 'NET HYDRA S'], 
      false, 
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

Formula_41_0 AS (

  SELECT 
    CAST(ABS(("QTY HYDRA B" - "QTY HYDRA S")) AS STRING) AS "QTY HYDRA NETO",
    CAST(ABS(("NET HYDRA B" - "NET HYDRA S")) AS STRING) AS "AMOUNT HYDRA NETO",
    *
  
  FROM Cleanse_38 AS in0

),

AlteryxSelect_42 AS (

  SELECT 
    CAST("QTY HYDRA NETO" AS FLOAT) AS "QTY HYDRA NETO",
    CAST("AMOUNT HYDRA NETO" AS FLOAT) AS "AMOUNT HYDRA NETO",
    * EXCLUDE ("QTY HYDRA B", "QTY HYDRA S", "NET HYDRA B", "NET HYDRA S", "QTY HYDRA NETO", "AMOUNT HYDRA NETO")
  
  FROM Formula_41_0 AS in0

)

SELECT *

FROM AlteryxSelect_42
