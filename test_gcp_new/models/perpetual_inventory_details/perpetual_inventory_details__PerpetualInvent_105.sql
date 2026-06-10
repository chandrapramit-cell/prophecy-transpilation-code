{{
  config({    
    "materialized": "table",
    "alias": "PerpetualInvent_105",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH DynamicInput_63 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'DynamicInput_63') }}

),

TextInput_33 AS (

  SELECT * 
  
  FROM {{ ref('seed_perpetual_inventory_details_33')}}

),

TextInput_33_cast AS (

  SELECT CAST(PartNumber AS string) AS PartNumber
  
  FROM TextInput_33 AS in0

),

AlteryxSelect_37 AS (

  SELECT 
    PartNumber AS PartNumber,
    * EXCEPT (`PartNumber`)
  
  FROM TextInput_33_cast AS in0

),

SamtecFacilitie_106 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'SamtecFacilitie_106') }}

),

AlteryxSelect_107 AS (

  SELECT 
    VALUE AS Facility,
    * EXCEPT (`NAME`, `VALUE`)
  
  FROM SamtecFacilitie_106 AS in0

),

Filter_23 AS (

  SELECT * 
  
  FROM AlteryxSelect_107 AS in0
  
  WHERE {{ var('variable23_Expression') }}

),

AlteryxSelect_24 AS (

  SELECT Facility AS Facility
  
  FROM Filter_23 AS in0

),

Join_64_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Facility`)
  
  FROM DynamicInput_63 AS in0
  INNER JOIN AlteryxSelect_24 AS in1
     ON (in0.Facility = in1.Facility)

),

Join_81_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`PartNumber`)
  
  FROM Join_64_inner AS in0
  INNER JOIN AlteryxSelect_37 AS in1
     ON (in0.PartNumber = in1.PartNumber)

),

AlteryxSelect_66 AS (

  SELECT 
    TransactionId AS TransactionId,
    TransactionType AS TransactionType,
    DateTransacted AS DateTransacted,
    TransactedBy AS TransactedBy,
    PartNumber AS PartNumber,
    Quantity AS Quantity,
    Note AS Note,
    Facility AS Facility,
    Plant AS Plant
  
  FROM Join_81_inner AS in0

),

DynamicInput_40 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'DynamicInput_40') }}

),

Join_43_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Facility`)
  
  FROM DynamicInput_40 AS in0
  INNER JOIN AlteryxSelect_24 AS in1
     ON (in0.Facility = in1.Facility)

),

Join_75_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`PartNumber`)
  
  FROM Join_43_inner AS in0
  INNER JOIN AlteryxSelect_37 AS in1
     ON (in0.PartNumber = in1.PartNumber)

),

Formula_44_0 AS (

  SELECT 
    CAST((((WarehouseOnHandBalance + BentecOnHandBalance) + WIPOnHandBalance) + ConsignedOnHandBalance) AS INTEGER) AS OpeningBalance,
    *
  
  FROM Join_75_inner AS in0

),

AlteryxSelect_41 AS (

  SELECT 
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber,
    OpeningBalance AS OpeningBalance
  
  FROM Formula_44_0 AS in0

),

Summarize_108 AS (

  SELECT 
    SUM(OpeningBalance) AS OpeningBalance,
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber
  
  FROM AlteryxSelect_41 AS in0
  
  GROUP BY 
    Facility, Plant, PartNumber

),

DynamicInput_49 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'DynamicInput_49') }}

),

Join_52_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Facility`)
  
  FROM DynamicInput_49 AS in0
  INNER JOIN AlteryxSelect_24 AS in1
     ON (in0.Facility = in1.Facility)

),

Join_76_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`PartNumber`)
  
  FROM Join_52_inner AS in0
  INNER JOIN AlteryxSelect_37 AS in1
     ON (in0.PartNumber = in1.PartNumber)

),

Formula_53_0 AS (

  SELECT 
    CAST((((WarehouseOnHandBalance + BentecOnHandBalance) + WIPOnHandBalance) + ConsignedOnHandBalance) AS INTEGER) AS ClosingBalance,
    *
  
  FROM Join_76_inner AS in0

),

AlteryxSelect_50 AS (

  SELECT 
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber,
    ClosingBalance AS ClosingBalance
  
  FROM Formula_53_0 AS in0

),

Summarize_109 AS (

  SELECT 
    SUM(ClosingBalance) AS ClosingBalance,
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber
  
  FROM AlteryxSelect_50 AS in0
  
  GROUP BY 
    Facility, Plant, PartNumber

),

Join_88_left_UnionFullOuter AS (

  SELECT 
    in0.OpeningBalance AS OpeningBalance,
    in1.ClosingBalance AS ClosingBalance,
    (
      CASE
        WHEN (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))
          THEN NULL
        ELSE in1.Plant
      END
    ) AS Plant,
    (
      CASE
        WHEN (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))
          THEN NULL
        ELSE in1.PartNumber
      END
    ) AS PartNumber,
    (
      CASE
        WHEN (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))
          THEN NULL
        ELSE in1.Facility
      END
    ) AS Facility,
    in0.* EXCEPT (`OpeningBalance`, `Plant`, `PartNumber`, `Facility`),
    in1.* EXCEPT (`ClosingBalance`, `Facility`, `Plant`, `PartNumber`)
  
  FROM Summarize_108 AS in0
  FULL JOIN Summarize_109 AS in1
     ON (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))

),

Join_91_left_UnionFullOuter AS (

  SELECT 
    in0.OpeningBalance AS OpeningBalance,
    in0.ClosingBalance AS ClosingBalance,
    in1.TransactionId AS TransactionId,
    in1.TransactionType AS TransactionType,
    in1.DateTransacted AS DateTransacted,
    in1.TransactedBy AS TransactedBy,
    in1.Quantity AS Quantity,
    in1.Note AS Note,
    (
      CASE
        WHEN (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))
          THEN NULL
        ELSE in1.Plant
      END
    ) AS Plant,
    (
      CASE
        WHEN (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))
          THEN NULL
        ELSE in1.PartNumber
      END
    ) AS PartNumber,
    (
      CASE
        WHEN (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))
          THEN NULL
        ELSE in1.Facility
      END
    ) AS Facility,
    in0.* EXCEPT (`OpeningBalance`, `ClosingBalance`, `Plant`, `PartNumber`, `Facility`),
    in1.* EXCEPT (`TransactionId`, 
    `TransactionType`, 
    `DateTransacted`, 
    `TransactedBy`, 
    `Quantity`, 
    `Note`, 
    `PartNumber`, 
    `Facility`, 
    `Plant`)
  
  FROM Join_88_left_UnionFullOuter AS in0
  FULL JOIN AlteryxSelect_66 AS in1
     ON (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))

),

Cleanse_94 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_91_left_UnionFullOuter'], 
      [
        { "name": "OpeningBalance", "dataType": "Bigint" }, 
        { "name": "ClosingBalance", "dataType": "Bigint" }, 
        { "name": "TransactionId", "dataType": "String" }, 
        { "name": "TransactionType", "dataType": "String" }, 
        { "name": "DateTransacted", "dataType": "Timestamp" }, 
        { "name": "TransactedBy", "dataType": "String" }, 
        { "name": "Quantity", "dataType": "Double" }, 
        { "name": "Note", "dataType": "String" }, 
        { "name": "Plant", "dataType": "String" }, 
        { "name": "PartNumber", "dataType": "String" }, 
        { "name": "Facility", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['OpeningBalance', 'ClosingBalance', 'Quantity'], 
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
    CAST(NULL AS INTEGER) AS RollingBalance,
    *
  
  FROM Cleanse_94 AS in0

),

MultiRowFormula_96_window AS (

  SELECT 
    *,
    lag(RollingBalance, 1) OVER (PARTITION BY Facility, Plant, PartNumber ORDER BY Facility ASC NULLS FIRST, Plant ASC NULLS FIRST, PartNumber ASC NULLS FIRST) AS RollingBalance_lag1
  
  FROM Formula_92_0 AS in0

),

MultiRowFormula_96 AS (

  {{
    prophecy_basics.ToDo(
      'Some(List((Facility,Ascending), (Plant,Ascending), (PartNumber,Ascending), (DateTransacted,Ascending))) (of class scala.Some)'
    )
  }}

),

AccountingTrans_98 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'AccountingTrans_98') }}

),

Join_99_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Base Transaction Type`)
  
  FROM MultiRowFormula_96 AS in0
  INNER JOIN AccountingTrans_98 AS in1
     ON (in0.TransactionType = in1.`Base Transaction Type`)

),

AlteryxSelect_103 AS (

  SELECT 
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber,
    OpeningBalance AS OpeningBalance,
    ClosingBalance AS ClosingBalance,
    TransactionId AS TransactionId,
    TransactionType AS SystemType,
    `Accounting Transaction Type` AS AccountingType,
    Quantity AS Quantity,
    RollingBalance AS RollingBalance,
    DateTransacted AS DateTransacted,
    TransactedBy AS TransactedBy,
    Note AS Note,
    * EXCEPT (`Facility`, 
    `Plant`, 
    `PartNumber`, 
    `OpeningBalance`, 
    `ClosingBalance`, 
    `TransactionId`, 
    `Quantity`, 
    `RollingBalance`, 
    `DateTransacted`, 
    `TransactedBy`, 
    `Note`, 
    `TransactionType`, 
    `Accounting Transaction Type`)
  
  FROM Join_99_inner AS in0

)

SELECT *

FROM AlteryxSelect_103
