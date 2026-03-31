{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DynamicInput_63 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'DynamicInput_63') }}

),

TextInput_33 AS (

  SELECT * 
  
  FROM {{ ref('seed_33')}}

),

TextInput_33_cast AS (

  SELECT CAST(PARTNUMBER AS string) AS PARTNUMBER
  
  FROM TextInput_33 AS in0

),

AlteryxSelect_37 AS (

  SELECT PARTNUMBER AS PARTNUMBER
  
  FROM TextInput_33_cast AS in0

),

SamtecFacilitie_106 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'SamtecFacilitie_106') }}

),

AlteryxSelect_107 AS (

  SELECT VALUE AS FACILITY
  
  FROM SamtecFacilitie_106 AS in0

),

Filter_23 AS (

  SELECT * 
  
  FROM AlteryxSelect_107 AS in0
  
  WHERE (FACILITY = 'SAMTEC USA')

),

AlteryxSelect_24 AS (

  SELECT FACILITY AS FACILITY
  
  FROM Filter_23 AS in0

),

Join_64_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("FACILITY")
  
  FROM DynamicInput_63 AS in0
  INNER JOIN AlteryxSelect_24 AS in1
     ON (in0.FACILITY = in1.FACILITY)

),

Join_81_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("PARTNUMBER")
  
  FROM Join_64_inner AS in0
  INNER JOIN AlteryxSelect_37 AS in1
     ON (in0.PARTNUMBER = in1.PARTNUMBER)

),

AlteryxSelect_66 AS (

  SELECT 
    TRANSACTIONID AS TRANSACTIONID,
    TRANSACTIONTYPE AS TRANSACTIONTYPE,
    DATETRANSACTED AS DATETRANSACTED,
    TRANSACTEDBY AS TRANSACTEDBY,
    PARTNUMBER AS PARTNUMBER,
    QUANTITY AS QUANTITY,
    NOTE AS NOTE,
    FACILITY AS FACILITY,
    PLANT AS PLANT
  
  FROM Join_81_inner AS in0

),

DynamicInput_40 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'DynamicInput_40') }}

),

Join_43_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("FACILITY")
  
  FROM DynamicInput_40 AS in0
  INNER JOIN AlteryxSelect_24 AS in1
     ON (in0.FACILITY = in1.FACILITY)

),

Join_75_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("PARTNUMBER")
  
  FROM Join_43_inner AS in0
  INNER JOIN AlteryxSelect_37 AS in1
     ON (in0.PARTNUMBER = in1.PARTNUMBER)

),

Formula_44_0 AS (

  SELECT 
    CAST((((WAREHOUSEONHANDBALANCE + BENTECONHANDBALANCE) + WIPONHANDBALANCE) + CONSIGNEDONHANDBALANCE) AS INTEGER) AS OPENINGBALANCE,
    *
  
  FROM Join_75_inner AS in0

),

AlteryxSelect_41 AS (

  SELECT 
    FACILITY AS FACILITY,
    PLANT AS PLANT,
    PARTNUMBER AS PARTNUMBER,
    OPENINGBALANCE AS OPENINGBALANCE
  
  FROM Formula_44_0 AS in0

),

Summarize_108 AS (

  SELECT 
    SUM(OPENINGBALANCE) AS OPENINGBALANCE,
    FACILITY AS FACILITY,
    PLANT AS PLANT,
    PARTNUMBER AS PARTNUMBER
  
  FROM AlteryxSelect_41 AS in0
  
  GROUP BY 
    FACILITY, PLANT, PARTNUMBER

),

DynamicInput_49 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'DynamicInput_49') }}

),

Join_52_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("FACILITY")
  
  FROM DynamicInput_49 AS in0
  INNER JOIN AlteryxSelect_24 AS in1
     ON (in0.FACILITY = in1.FACILITY)

),

Join_76_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("PARTNUMBER")
  
  FROM Join_52_inner AS in0
  INNER JOIN AlteryxSelect_37 AS in1
     ON (in0.PARTNUMBER = in1.PARTNUMBER)

),

Formula_53_0 AS (

  SELECT 
    CAST((((WAREHOUSEONHANDBALANCE + BENTECONHANDBALANCE) + WIPONHANDBALANCE) + CONSIGNEDONHANDBALANCE) AS INTEGER) AS CLOSINGBALANCE,
    *
  
  FROM Join_76_inner AS in0

),

AlteryxSelect_50 AS (

  SELECT 
    FACILITY AS FACILITY,
    PLANT AS PLANT,
    PARTNUMBER AS PARTNUMBER,
    CLOSINGBALANCE AS CLOSINGBALANCE
  
  FROM Formula_53_0 AS in0

),

Summarize_109 AS (

  SELECT 
    SUM(CLOSINGBALANCE) AS CLOSINGBALANCE,
    FACILITY AS FACILITY,
    PLANT AS PLANT,
    PARTNUMBER AS PARTNUMBER
  
  FROM AlteryxSelect_50 AS in0
  
  GROUP BY 
    FACILITY, PLANT, PARTNUMBER

),

Join_88_left_UnionFullOuter AS (

  SELECT 
    in0.OPENINGBALANCE AS OPENINGBALANCE,
    in1.CLOSINGBALANCE AS CLOSINGBALANCE,
    (
      CASE
        WHEN (((in0.FACILITY = in1.FACILITY) AND (in0.PLANT = in1.PLANT)) AND (in0.PARTNUMBER = in1.PARTNUMBER))
          THEN NULL
        ELSE in1.PLANT
      END
    ) AS PLANT,
    (
      CASE
        WHEN (((in0.FACILITY = in1.FACILITY) AND (in0.PLANT = in1.PLANT)) AND (in0.PARTNUMBER = in1.PARTNUMBER))
          THEN NULL
        ELSE in1.PARTNUMBER
      END
    ) AS PARTNUMBER,
    (
      CASE
        WHEN (((in0.FACILITY = in1.FACILITY) AND (in0.PLANT = in1.PLANT)) AND (in0.PARTNUMBER = in1.PARTNUMBER))
          THEN NULL
        ELSE in1.FACILITY
      END
    ) AS FACILITY,
    in0.* EXCLUDE ("OPENINGBALANCE", "PLANT", "PARTNUMBER", "FACILITY"),
    in1.* EXCLUDE ("CLOSINGBALANCE", "FACILITY", "PLANT", "PARTNUMBER")
  
  FROM Summarize_108 AS in0
  FULL JOIN Summarize_109 AS in1
     ON (((in0.FACILITY = in1.FACILITY) AND (in0.PLANT = in1.PLANT)) AND (in0.PARTNUMBER = in1.PARTNUMBER))

),

Join_91_left_UnionFullOuter AS (

  SELECT 
    in0.OPENINGBALANCE AS OPENINGBALANCE,
    in0.CLOSINGBALANCE AS CLOSINGBALANCE,
    in1.TRANSACTIONID AS TRANSACTIONID,
    in1.TRANSACTIONTYPE AS TRANSACTIONTYPE,
    in1.DATETRANSACTED AS DATETRANSACTED,
    in1.TRANSACTEDBY AS TRANSACTEDBY,
    in1.QUANTITY AS QUANTITY,
    in1.NOTE AS NOTE,
    (
      CASE
        WHEN (((in0.FACILITY = in1.FACILITY) AND (in0.PLANT = in1.PLANT)) AND (in0.PARTNUMBER = in1.PARTNUMBER))
          THEN NULL
        ELSE in1.PLANT
      END
    ) AS PLANT,
    (
      CASE
        WHEN (((in0.FACILITY = in1.FACILITY) AND (in0.PLANT = in1.PLANT)) AND (in0.PARTNUMBER = in1.PARTNUMBER))
          THEN NULL
        ELSE in1.PARTNUMBER
      END
    ) AS PARTNUMBER,
    (
      CASE
        WHEN (((in0.FACILITY = in1.FACILITY) AND (in0.PLANT = in1.PLANT)) AND (in0.PARTNUMBER = in1.PARTNUMBER))
          THEN NULL
        ELSE in1.FACILITY
      END
    ) AS FACILITY,
    in0.* EXCLUDE ("OPENINGBALANCE", "CLOSINGBALANCE", "PLANT", "PARTNUMBER", "FACILITY"),
    in1.* EXCLUDE ("TRANSACTIONID", 
    "TRANSACTIONTYPE", 
    "DATETRANSACTED", 
    "TRANSACTEDBY", 
    "QUANTITY", 
    "NOTE", 
    "PARTNUMBER", 
    "FACILITY", 
    "PLANT")
  
  FROM Join_88_left_UnionFullOuter AS in0
  FULL JOIN AlteryxSelect_66 AS in1
     ON (((in0.FACILITY = in1.FACILITY) AND (in0.PLANT = in1.PLANT)) AND (in0.PARTNUMBER = in1.PARTNUMBER))

),

Cleanse_94 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_91_left_UnionFullOuter'], 
      [
        { "name": "OPENINGBALANCE", "dataType": "Number" }, 
        { "name": "CLOSINGBALANCE", "dataType": "Number" }, 
        { "name": "TRANSACTIONID", "dataType": "String" }, 
        { "name": "TRANSACTIONTYPE", "dataType": "String" }, 
        { "name": "DATETRANSACTED", "dataType": "Timestamp" }, 
        { "name": "TRANSACTEDBY", "dataType": "String" }, 
        { "name": "QUANTITY", "dataType": "Float" }, 
        { "name": "NOTE", "dataType": "String" }, 
        { "name": "PLANT", "dataType": "String" }, 
        { "name": "PARTNUMBER", "dataType": "String" }, 
        { "name": "FACILITY", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['OPENINGBALANCE', 'CLOSINGBALANCE', 'QUANTITY'], 
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

Formula_92_0 AS (

  SELECT 
    CAST(NULL AS INTEGER) AS ROLLINGBALANCE,
    *
  
  FROM Cleanse_94 AS in0

),

MultiRowFormula_96_window AS (

  SELECT 
    *,
    LAG(ROLLINGBALANCE, 1) OVER (PARTITION BY FACILITY, PLANT, PARTNUMBER ORDER BY FACILITY ASC NULLS FIRST, PLANT ASC NULLS FIRST, PARTNUMBER ASC NULLS FIRST) AS ROLLINGBALANCE_LAG1
  
  FROM Formula_92_0 AS in0

),

AccountingTrans_98 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'AccountingTrans_98') }}

),

MultiRowFormula_96_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (ROLLINGBALANCE_LAG1 IS NULL)
          THEN (OPENINGBALANCE + QUANTITY)
        ELSE (ROLLINGBALANCE_LAG1 + QUANTITY)
      END
    ) AS INTEGER) AS ROLLINGBALANCE,
    * EXCLUDE ("ROLLINGBALANCE_LAG1", "ROLLINGBALANCE")
  
  FROM MultiRowFormula_96_window AS in0

),

Join_99_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("BASE TRANSACTION TYPE")
  
  FROM MultiRowFormula_96_0 AS in0
  INNER JOIN AccountingTrans_98 AS in1
     ON in0.TRANSACTIONTYPE = in1."BASE TRANSACTION TYPE"

),

AlteryxSelect_103 AS (

  SELECT 
    FACILITY AS FACILITY,
    PLANT AS PLANT,
    PARTNUMBER AS PARTNUMBER,
    OPENINGBALANCE AS OPENINGBALANCE,
    CLOSINGBALANCE AS CLOSINGBALANCE,
    TRANSACTIONID AS TRANSACTIONID,
    TRANSACTIONTYPE AS SYSTEMTYPE,
    "ACCOUNTING TRANSACTION TYPE" AS ACCOUNTINGTYPE,
    QUANTITY AS QUANTITY,
    ROLLINGBALANCE AS ROLLINGBALANCE,
    DATETRANSACTED AS DATETRANSACTED,
    TRANSACTEDBY AS TRANSACTEDBY,
    NOTE AS NOTE
  
  FROM Join_99_inner AS in0

)

SELECT *

FROM AlteryxSelect_103
