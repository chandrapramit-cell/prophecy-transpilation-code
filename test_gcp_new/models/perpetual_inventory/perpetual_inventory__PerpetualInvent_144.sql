{{
  config({    
    "materialized": "table",
    "alias": "PerpetualInvent_144",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH DynamicInput_164 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'DynamicInput_164') }}

),

Cleanse_108 AS (

  {{
    prophecy_basics.DataCleansing(
      ['DynamicInput_164'], 
      [
        { "name": "PartNumber", "dataType": "String" }, 
        { "name": "CustomerSupplied", "dataType": "String" }, 
        { "name": "UnitCost", "dataType": "Decimal" }, 
        { "name": "Buyer", "dataType": "String" }, 
        { "name": "PlatingCost", "dataType": "Decimal" }, 
        { "name": "OrderPolicy", "dataType": "String" }, 
        { "name": "LaborCost", "dataType": "Decimal" }, 
        { "name": "StandardCost", "dataType": "Decimal" }, 
        { "name": "PlannerCode", "dataType": "String" }, 
        { "name": "StandardLaborCost", "dataType": "Decimal" }, 
        { "name": "StandardPlatingCost", "dataType": "Decimal" }
      ], 
      'keepOriginal', 
      [
        'PartNumber', 
        'UnitCost', 
        'LaborCost', 
        'PlatingCost', 
        'PlannerCode', 
        'Buyer', 
        'OrderPolicy', 
        'CustomerSupplied'
      ], 
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

AlteryxSelect_109 AS (

  SELECT 
    PartNumber AS PartNumber,
    UnitCost AS UnitCost,
    LaborCost AS LaborCost,
    PlatingCost AS PlatingCost,
    PlannerCode AS PlannerCode,
    Buyer AS Buyer,
    OrderPolicy AS OrderPolicy,
    CustomerSupplied AS CustomerSupplied,
    StandardCost AS StandardCost,
    StandardLaborCost AS StandardLaborCost,
    StandardPlatingCost AS StandardPlatingCost
  
  FROM Cleanse_108 AS in0

),

AlteryxSelect_113 AS (

  SELECT 
    UnitCost AS PreviousQuarterUnitCost,
    LaborCost AS PreviousQuarterLaborCost,
    PlatingCost AS PreviousQuarterPlatingCost,
    PlannerCode AS PreviousQuarterPlannerCode,
    Buyer AS PreviousQuarterBuyer,
    OrderPolicy AS PreviousQuarterOrderPolicy,
    CustomerSupplied AS PreviousQuarterCustomerSupplied,
    StandardCost AS PreviousQuarterStandardUnitCost,
    StandardLaborCost AS PreviousQuarterStandardLaborCost,
    StandardPlatingCost AS PreviousQuarterStandardPlatingCost,
    * EXCEPT (`UnitCost`, 
    `LaborCost`, 
    `PlatingCost`, 
    `PlannerCode`, 
    `Buyer`, 
    `OrderPolicy`, 
    `CustomerSupplied`, 
    `StandardCost`, 
    `StandardLaborCost`, 
    `StandardPlatingCost`)
  
  FROM AlteryxSelect_109 AS in0

),

AlteryxSelect_23 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory__AlteryxSelect_23')}}

),

DynamicInput_33 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'DynamicInput_33') }}

),

Join_36_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Facility`)
  
  FROM DynamicInput_33 AS in0
  INNER JOIN AlteryxSelect_23 AS in1
     ON (in0.Facility = in1.Facility)

),

Formula_39_0 AS (

  SELECT 
    CAST((((WarehouseOnHandBalance + BentecOnHandBalance) + WIPOnHandBalance) + ConsignedOnHandBalance) AS INTEGER) AS ClosingBalance,
    *
  
  FROM Join_36_inner AS in0

),

AlteryxSelect_34 AS (

  SELECT 
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber,
    ClosingBalance AS ClosingBalance
  
  FROM Formula_39_0 AS in0

),

Summarize_158 AS (

  SELECT 
    SUM(ClosingBalance) AS ClosingBalance,
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber
  
  FROM AlteryxSelect_34 AS in0
  
  GROUP BY 
    Facility, Plant, PartNumber

),

DSN_r2s_prod_ed_181 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'DSN_r2s_prod_ed_181') }}

),

AlteryxSelect_182 AS (

  SELECT 
    part_number AS PartNumber,
    reporting_series AS ReportingSeries,
    reporting_series_era AS ReportingSeriesProductEra,
    reporting_series_weave AS ReportingSeriesProductWeave,
    reporting_series_block AS ReportingSeriesProductSolutionBlock,
    reporting_series_segment AS ReportingSeriesProductSegment,
    dm_item_type AS ItemType,
    pcmfg_item_type AS PCMfgItemType,
    * EXCEPT (`part_number`, 
    `reporting_series`, 
    `reporting_series_era`, 
    `reporting_series_weave`, 
    `reporting_series_block`, 
    `reporting_series_segment`, 
    `dm_item_type`, 
    `pcmfg_item_type`)
  
  FROM DSN_r2s_prod_ed_181 AS in0

),

AccountingTrans_82 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'AccountingTrans_82') }}

),

CrossTab_81_rename AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'CrossTab_81_rename') }}

),

AlteryxSelect_85 AS (

  SELECT 
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber,
    * EXCEPT (`Facility`, `Plant`, `PartNumber`)
  
  FROM CrossTab_81_rename AS in0

),

Union_83 AS (

  {{
    prophecy_basics.UnionByName(
      ['AccountingTrans_82', 'AlteryxSelect_85'], 
      [
        '[{"name": "Produced", "dataType": "Integer"}, {"name": "PartNumber", "dataType": "String"}, {"name": "TransferredOut", "dataType": "Integer"}, {"name": "Adjusted", "dataType": "Integer"}, {"name": "TransferredIn", "dataType": "Integer"}, {"name": "Purchased", "dataType": "Integer"}, {"name": "Sold", "dataType": "Integer"}, {"name": "Issued", "dataType": "Integer"}]', 
        '[{"name": "Facility", "dataType": "String"}, {"name": "Plant", "dataType": "String"}, {"name": "PartNumber", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_86 AS (

  SELECT 
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber,
    Purchased AS Purchased,
    Produced AS Produced,
    Sold AS Sold,
    Issued AS Issued,
    Adjusted AS Adjusted,
    TransferredIn AS TransferredIn,
    TransferredOut AS TransferredOut,
    * EXCEPT (`Facility`, 
    `Plant`, 
    `PartNumber`, 
    `Purchased`, 
    `Produced`, 
    `Sold`, 
    `Issued`, 
    `Adjusted`, 
    `TransferredIn`, 
    `TransferredOut`)
  
  FROM Union_83 AS in0

),

Cleanse_84 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_86'], 
      [
        { "name": "Facility", "dataType": "String" }, 
        { "name": "Plant", "dataType": "String" }, 
        { "name": "PartNumber", "dataType": "String" }, 
        { "name": "Purchased", "dataType": "Integer" }, 
        { "name": "Produced", "dataType": "Integer" }, 
        { "name": "Sold", "dataType": "Integer" }, 
        { "name": "Issued", "dataType": "Integer" }, 
        { "name": "Adjusted", "dataType": "Integer" }, 
        { "name": "TransferredIn", "dataType": "Integer" }, 
        { "name": "TransferredOut", "dataType": "Integer" }
      ], 
      'keepOriginal', 
      ['Purchased', 'Produced', 'Sold', 'Issued', 'Adjusted', 'TransferredIn', 'TransferredOut'], 
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

DynamicInput_159 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'DynamicInput_159') }}

),

Cleanse_100 AS (

  {{
    prophecy_basics.DataCleansing(
      ['DynamicInput_159'], 
      [
        { "name": "PartNumber", "dataType": "String" }, 
        { "name": "CustomerSupplied", "dataType": "String" }, 
        { "name": "UnitCost", "dataType": "Decimal" }, 
        { "name": "Buyer", "dataType": "String" }, 
        { "name": "PlatingCost", "dataType": "Decimal" }, 
        { "name": "OrderPolicy", "dataType": "String" }, 
        { "name": "LaborCost", "dataType": "Decimal" }, 
        { "name": "StandardCost", "dataType": "Decimal" }, 
        { "name": "PlannerCode", "dataType": "String" }, 
        { "name": "StandardLaborCost", "dataType": "Decimal" }, 
        { "name": "StandardPlatingCost", "dataType": "Decimal" }
      ], 
      'keepOriginal', 
      [
        'PartNumber', 
        'UnitCost', 
        'LaborCost', 
        'PlatingCost', 
        'PlannerCode', 
        'Buyer', 
        'OrderPolicy', 
        'CustomerSupplied'
      ], 
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

DynamicInput_28 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'DynamicInput_28') }}

),

Join_31_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Facility`)
  
  FROM DynamicInput_28 AS in0
  INNER JOIN AlteryxSelect_23 AS in1
     ON (in0.Facility = in1.Facility)

),

Formula_38_0 AS (

  SELECT 
    CAST((((WarehouseOnHandBalance + BentecOnHandBalance) + WIPOnHandBalance) + ConsignedOnHandBalance) AS INTEGER) AS OpeningBalance,
    *
  
  FROM Join_31_inner AS in0

),

AlteryxSelect_29 AS (

  SELECT 
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber,
    OpeningBalance AS OpeningBalance
  
  FROM Formula_38_0 AS in0

),

Summarize_157 AS (

  SELECT 
    SUM(OpeningBalance) AS OpeningBalance,
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber
  
  FROM AlteryxSelect_29 AS in0
  
  GROUP BY 
    Facility, Plant, PartNumber

),

Join_43_left_UnionFullOuter AS (

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
  
  FROM Summarize_157 AS in0
  FULL JOIN Summarize_158 AS in1
     ON (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))

),

Join_87_left_UnionFullOuter AS (

  SELECT 
    in0.OpeningBalance AS OpeningBalance,
    in0.ClosingBalance AS ClosingBalance,
    in1.Purchased AS Purchased,
    in1.Produced AS Produced,
    in1.Sold AS Sold,
    in1.Issued AS Issued,
    in1.Adjusted AS Adjusted,
    in1.TransferredIn AS TransferredIn,
    in1.TransferredOut AS TransferredOut,
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
    in1.* EXCEPT (`Purchased`, 
    `Produced`, 
    `Sold`, 
    `Issued`, 
    `Adjusted`, 
    `TransferredIn`, 
    `TransferredOut`, 
    `Facility`, 
    `Plant`, 
    `PartNumber`)
  
  FROM Join_43_left_UnionFullOuter AS in0
  FULL JOIN Cleanse_84 AS in1
     ON (((in0.Facility = in1.Facility) AND (in0.Plant = in1.Plant)) AND (in0.PartNumber = in1.PartNumber))

),

AlteryxSelect_90 AS (

  SELECT 
    Facility AS Facility,
    Plant AS Plant,
    PartNumber AS PartNumber,
    OpeningBalance AS OpeningBalance,
    Purchased AS Purchased,
    Produced AS Produced,
    Sold AS Sold,
    Issued AS Issued,
    Adjusted AS Adjusted,
    TransferredIn AS TransferredIn,
    TransferredOut AS TransferredOut,
    ClosingBalance AS ClosingBalance,
    * EXCEPT (`Facility`, 
    `Plant`, 
    `PartNumber`, 
    `OpeningBalance`, 
    `Purchased`, 
    `Produced`, 
    `Sold`, 
    `Issued`, 
    `Adjusted`, 
    `TransferredIn`, 
    `TransferredOut`, 
    `ClosingBalance`)
  
  FROM Join_87_left_UnionFullOuter AS in0

),

Cleanse_94 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_90'], 
      [
        { "name": "Facility", "dataType": "String" }, 
        { "name": "Plant", "dataType": "String" }, 
        { "name": "PartNumber", "dataType": "String" }, 
        { "name": "OpeningBalance", "dataType": "Bigint" }, 
        { "name": "Purchased", "dataType": "Integer" }, 
        { "name": "Produced", "dataType": "Integer" }, 
        { "name": "Sold", "dataType": "Integer" }, 
        { "name": "Issued", "dataType": "Integer" }, 
        { "name": "Adjusted", "dataType": "Integer" }, 
        { "name": "TransferredIn", "dataType": "Integer" }, 
        { "name": "TransferredOut", "dataType": "Integer" }, 
        { "name": "ClosingBalance", "dataType": "Bigint" }
      ], 
      'keepOriginal', 
      [
        'OpeningBalance', 
        'Purchased', 
        'Produced', 
        'Sold', 
        'Issued', 
        'Adjusted', 
        'TransferredIn', 
        'TransferredOut', 
        'ClosingBalance'
      ], 
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

Formula_91_0 AS (

  SELECT 
    CAST((
      ((((((OpeningBalance + Purchased) + Produced) + Sold) + Issued) + Adjusted) + TransferredIn)
      + TransferredOut
    ) AS INTEGER) AS TransactedClosingBalance,
    *
  
  FROM Cleanse_94 AS in0

),

Formula_91_1 AS (

  SELECT 
    CAST((ABS((TransactedClosingBalance - ClosingBalance)) <= 1) AS BOOLEAN) AS Reconciles,
    CAST((TransactedClosingBalance - ClosingBalance) AS INTEGER) AS Discrepancy,
    *
  
  FROM Formula_91_0 AS in0

),

AccountingTrans_70 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'AccountingTrans_70') }}

),

AlteryxSelect_72 AS (

  SELECT 
    PlannerCode AS PlannerCode,
    AccountingPlannerCode AS AccountingPlannerCode,
    * EXCEPT (`PlannerCode`, `AccountingPlannerCode`)
  
  FROM AccountingTrans_70 AS in0

),

DynamicInput_161 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'DynamicInput_161') }}

),

Cleanse_104 AS (

  {{
    prophecy_basics.DataCleansing(
      ['DynamicInput_161'], 
      [
        { "name": "PartNumber", "dataType": "String" }, 
        { "name": "CustomerSupplied", "dataType": "String" }, 
        { "name": "UnitCost", "dataType": "Decimal" }, 
        { "name": "Buyer", "dataType": "String" }, 
        { "name": "PlatingCost", "dataType": "Decimal" }, 
        { "name": "OrderPolicy", "dataType": "String" }, 
        { "name": "LaborCost", "dataType": "Decimal" }, 
        { "name": "StandardCost", "dataType": "Decimal" }, 
        { "name": "PlannerCode", "dataType": "String" }, 
        { "name": "StandardLaborCost", "dataType": "Decimal" }, 
        { "name": "StandardPlatingCost", "dataType": "Decimal" }
      ], 
      'keepOriginal', 
      [
        'PartNumber', 
        'UnitCost', 
        'LaborCost', 
        'PlatingCost', 
        'PlannerCode', 
        'Buyer', 
        'OrderPolicy', 
        'CustomerSupplied'
      ], 
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

AlteryxSelect_105 AS (

  SELECT 
    PartNumber AS PartNumber,
    UnitCost AS UnitCost,
    LaborCost AS LaborCost,
    PlatingCost AS PlatingCost,
    PlannerCode AS PlannerCode,
    Buyer AS Buyer,
    OrderPolicy AS OrderPolicy,
    CustomerSupplied AS CustomerSupplied,
    StandardCost AS StandardCost,
    StandardLaborCost AS StandardLaborCost,
    StandardPlatingCost AS StandardPlatingCost
  
  FROM Cleanse_104 AS in0

),

AlteryxSelect_112 AS (

  SELECT 
    UnitCost AS ClosingUnitCost,
    LaborCost AS ClosingLaborCost,
    PlatingCost AS ClostingPlatingCost,
    PlannerCode AS ClosingPlannerCode,
    Buyer AS ClosingBuyer,
    OrderPolicy AS ClosingOrderPolicy,
    CustomerSupplied AS ClosingCustomerSupplied,
    StandardCost AS ClosingStandardUnitCost,
    StandardLaborCost AS ClosingStandardLaborCost,
    StandardPlatingCost AS ClosingStandardPlatingCost,
    * EXCEPT (`UnitCost`, 
    `LaborCost`, 
    `PlatingCost`, 
    `PlannerCode`, 
    `Buyer`, 
    `OrderPolicy`, 
    `CustomerSupplied`, 
    `StandardCost`, 
    `StandardLaborCost`, 
    `StandardPlatingCost`)
  
  FROM AlteryxSelect_105 AS in0

),

AlteryxSelect_101 AS (

  SELECT 
    PartNumber AS PartNumber,
    UnitCost AS UnitCost,
    LaborCost AS LaborCost,
    PlatingCost AS PlatingCost,
    PlannerCode AS PlannerCode,
    Buyer AS Buyer,
    OrderPolicy AS OrderPolicy,
    CustomerSupplied AS CustomerSupplied,
    StandardCost AS StandardCost,
    StandardLaborCost AS StandardLaborCost,
    StandardPlatingCost AS StandardPlatingCost
  
  FROM Cleanse_100 AS in0

),

AlteryxSelect_111 AS (

  SELECT 
    UnitCost AS OpeningUnitCost,
    LaborCost AS OpeningLaborCost,
    PlatingCost AS OpeningPlatingCost,
    PlannerCode AS OpeningPlannerCode,
    Buyer AS OpeningBuyer,
    OrderPolicy AS OpeningOrderPolicy,
    CustomerSupplied AS OpeningCustomerSupplied,
    StandardCost AS OpeningStandardUnitCost,
    StandardLaborCost AS OpeningStandardLaborCost,
    StandardPlatingCost AS OpeningStandardPlatingCost,
    * EXCEPT (`UnitCost`, 
    `LaborCost`, 
    `PlatingCost`, 
    `PlannerCode`, 
    `Buyer`, 
    `OrderPolicy`, 
    `CustomerSupplied`, 
    `StandardCost`, 
    `StandardLaborCost`, 
    `StandardPlatingCost`)
  
  FROM AlteryxSelect_101 AS in0

),

Join_114_left_UnionFullOuter AS (

  SELECT 
    in0.OpeningUnitCost AS OpeningUnitCost,
    in0.OpeningLaborCost AS OpeningLaborCost,
    in0.OpeningPlannerCode AS OpeningPlannerCode,
    in1.ClosingUnitCost AS ClosingUnitCost,
    in1.ClosingLaborCost AS ClosingLaborCost,
    in1.ClosingPlannerCode AS ClosingPlannerCode,
    in0.OpeningPlatingCost AS OpeningPlatingCost,
    in1.ClostingPlatingCost AS ClostingPlatingCost,
    in0.OpeningBuyer AS OpeningBuyer,
    in1.ClosingBuyer AS ClosingBuyer,
    in0.OpeningOrderPolicy AS OpeningOrderPolicy,
    in1.ClosingOrderPolicy AS ClosingOrderPolicy,
    in0.OpeningCustomerSupplied AS OpeningCustomerSupplied,
    in1.ClosingCustomerSupplied AS ClosingCustomerSupplied,
    in0.OpeningStandardUnitCost AS OpeningStandardUnitCost,
    in0.OpeningStandardLaborCost AS OpeningStandardLaborCost,
    in0.OpeningStandardPlatingCost AS OpeningStandardPlatingCost,
    in1.ClosingStandardUnitCost AS ClosingStandardUnitCost,
    in1.ClosingStandardLaborCost AS ClosingStandardLaborCost,
    in1.ClosingStandardPlatingCost AS ClosingStandardPlatingCost,
    (
      CASE
        WHEN (in0.PartNumber = in1.PartNumber)
          THEN NULL
        ELSE in1.PartNumber
      END
    ) AS PartNumber,
    in0.* EXCEPT (`OpeningUnitCost`, 
    `OpeningLaborCost`, 
    `OpeningPlannerCode`, 
    `OpeningPlatingCost`, 
    `OpeningBuyer`, 
    `OpeningOrderPolicy`, 
    `OpeningCustomerSupplied`, 
    `OpeningStandardUnitCost`, 
    `OpeningStandardLaborCost`, 
    `OpeningStandardPlatingCost`, 
    `PartNumber`),
    in1.* EXCEPT (`ClosingUnitCost`, 
    `ClosingLaborCost`, 
    `ClosingPlannerCode`, 
    `PartNumber`, 
    `ClostingPlatingCost`, 
    `ClosingBuyer`, 
    `ClosingOrderPolicy`, 
    `ClosingCustomerSupplied`, 
    `ClosingStandardUnitCost`, 
    `ClosingStandardLaborCost`, 
    `ClosingStandardPlatingCost`)
  
  FROM AlteryxSelect_111 AS in0
  FULL JOIN AlteryxSelect_112 AS in1
     ON (in0.PartNumber = in1.PartNumber)

),

Join_115_left_UnionFullOuter AS (

  SELECT 
    in0.OpeningUnitCost AS OpeningUnitCost,
    in0.OpeningLaborCost AS OpeningLaborCost,
    in0.OpeningPlannerCode AS OpeningPlannerCode,
    in0.ClosingUnitCost AS ClosingUnitCost,
    in0.ClosingLaborCost AS ClosingLaborCost,
    in0.ClosingPlannerCode AS ClosingPlannerCode,
    in1.PreviousQuarterUnitCost AS PreviousQuarterUnitCost,
    in1.PreviousQuarterLaborCost AS PreviousQuarterLaborCost,
    in1.PreviousQuarterPlannerCode AS PreviousQuarterPlannerCode,
    in0.OpeningPlatingCost AS OpeningPlatingCost,
    in0.ClostingPlatingCost AS ClostingPlatingCost,
    in1.PreviousQuarterPlatingCost AS PreviousQuarterPlatingCost,
    in0.OpeningBuyer AS OpeningBuyer,
    in0.ClosingBuyer AS ClosingBuyer,
    in1.PreviousQuarterBuyer AS PreviousQuarterBuyer,
    in0.OpeningOrderPolicy AS OpeningOrderPolicy,
    in0.ClosingOrderPolicy AS ClosingOrderPolicy,
    in1.PreviousQuarterOrderPolicy AS PreviousQuarterOrderPolicy,
    in0.OpeningCustomerSupplied AS OpeningCustomerSupplied,
    in0.ClosingCustomerSupplied AS ClosingCustomerSupplied,
    in0.OpeningStandardUnitCost AS OpeningStandardUnitCost,
    in0.OpeningStandardLaborCost AS OpeningStandardLaborCost,
    in0.OpeningStandardPlatingCost AS OpeningStandardPlatingCost,
    in0.ClosingStandardUnitCost AS ClosingStandardUnitCost,
    in0.ClosingStandardLaborCost AS ClosingStandardLaborCost,
    in0.ClosingStandardPlatingCost AS ClosingStandardPlatingCost,
    in1.PreviousQuarterCustomerSupplied AS PreviousQuarterCustomerSupplied,
    in1.PreviousQuarterStandardUnitCost AS PreviousQuarterStandardUnitCost,
    in1.PreviousQuarterStandardLaborCost AS PreviousQuarterStandardLaborCost,
    in1.PreviousQuarterStandardPlatingCost AS PreviousQuarterStandardPlatingCost,
    (
      CASE
        WHEN (in0.PartNumber = in1.PartNumber)
          THEN NULL
        ELSE in1.PartNumber
      END
    ) AS PartNumber,
    in0.* EXCEPT (`OpeningUnitCost`, 
    `OpeningLaborCost`, 
    `OpeningPlannerCode`, 
    `ClosingUnitCost`, 
    `ClosingLaborCost`, 
    `ClosingPlannerCode`, 
    `OpeningPlatingCost`, 
    `ClostingPlatingCost`, 
    `OpeningBuyer`, 
    `ClosingBuyer`, 
    `OpeningOrderPolicy`, 
    `ClosingOrderPolicy`, 
    `OpeningCustomerSupplied`, 
    `ClosingCustomerSupplied`, 
    `OpeningStandardUnitCost`, 
    `OpeningStandardLaborCost`, 
    `OpeningStandardPlatingCost`, 
    `ClosingStandardUnitCost`, 
    `ClosingStandardLaborCost`, 
    `ClosingStandardPlatingCost`, 
    `PartNumber`),
    in1.* EXCEPT (`PreviousQuarterUnitCost`, 
    `PreviousQuarterLaborCost`, 
    `PreviousQuarterPlannerCode`, 
    `PartNumber`, 
    `PreviousQuarterPlatingCost`, 
    `PreviousQuarterBuyer`, 
    `PreviousQuarterOrderPolicy`, 
    `PreviousQuarterCustomerSupplied`, 
    `PreviousQuarterStandardUnitCost`, 
    `PreviousQuarterStandardLaborCost`, 
    `PreviousQuarterStandardPlatingCost`)
  
  FROM Join_114_left_UnionFullOuter AS in0
  FULL JOIN AlteryxSelect_113 AS in1
     ON (in0.PartNumber = in1.PartNumber)

),

Cleanse_123 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_115_left_UnionFullOuter'], 
      [
        { "name": "OpeningUnitCost", "dataType": "Decimal" }, 
        { "name": "OpeningLaborCost", "dataType": "Decimal" }, 
        { "name": "OpeningPlannerCode", "dataType": "String" }, 
        { "name": "ClosingUnitCost", "dataType": "Decimal" }, 
        { "name": "ClosingLaborCost", "dataType": "Decimal" }, 
        { "name": "ClosingPlannerCode", "dataType": "String" }, 
        { "name": "PreviousQuarterUnitCost", "dataType": "Decimal" }, 
        { "name": "PreviousQuarterLaborCost", "dataType": "Decimal" }, 
        { "name": "PreviousQuarterPlannerCode", "dataType": "String" }, 
        { "name": "OpeningPlatingCost", "dataType": "Decimal" }, 
        { "name": "ClostingPlatingCost", "dataType": "Decimal" }, 
        { "name": "PreviousQuarterPlatingCost", "dataType": "Decimal" }, 
        { "name": "OpeningBuyer", "dataType": "String" }, 
        { "name": "ClosingBuyer", "dataType": "String" }, 
        { "name": "PreviousQuarterBuyer", "dataType": "String" }, 
        { "name": "OpeningOrderPolicy", "dataType": "String" }, 
        { "name": "ClosingOrderPolicy", "dataType": "String" }, 
        { "name": "PreviousQuarterOrderPolicy", "dataType": "String" }, 
        { "name": "OpeningCustomerSupplied", "dataType": "String" }, 
        { "name": "ClosingCustomerSupplied", "dataType": "String" }, 
        { "name": "OpeningStandardUnitCost", "dataType": "Decimal" }, 
        { "name": "OpeningStandardLaborCost", "dataType": "Decimal" }, 
        { "name": "OpeningStandardPlatingCost", "dataType": "Decimal" }, 
        { "name": "ClosingStandardUnitCost", "dataType": "Decimal" }, 
        { "name": "ClosingStandardLaborCost", "dataType": "Decimal" }, 
        { "name": "ClosingStandardPlatingCost", "dataType": "Decimal" }, 
        { "name": "PreviousQuarterCustomerSupplied", "dataType": "String" }, 
        { "name": "PreviousQuarterStandardUnitCost", "dataType": "Decimal" }, 
        { "name": "PreviousQuarterStandardLaborCost", "dataType": "Decimal" }, 
        { "name": "PreviousQuarterStandardPlatingCost", "dataType": "Decimal" }, 
        { "name": "PartNumber", "dataType": "String" }
      ], 
      'keepOriginal', 
      [
        'PartNumber', 
        'OpeningUnitCost', 
        'OpeningLaborCost', 
        'OpeningPlannerCode', 
        'ClosingUnitCost', 
        'ClosingLaborCost', 
        'ClosingPlannerCode', 
        'PreviousQuarterUnitCost', 
        'PreviousQuarterLaborCost', 
        'PreviousQuarterPlannerCode', 
        'OpeningPlatingCost', 
        'ClostingPlatingCost', 
        'PreviousQuarterPlatingCost', 
        'OpeningBuyer', 
        'ClosingBuyer', 
        'PreviousQuarterBuyer', 
        'OpeningOrderPolicy', 
        'ClosingOrderPolicy', 
        'PreviousQuarterOrderPolicy'
      ], 
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

Join_127_left_UnionLeftOuter AS (

  SELECT 
    in0.PartNumber AS PartNumber,
    in0.OpeningUnitCost AS OpeningUnitCost,
    in0.OpeningLaborCost AS OpeningLaborCost,
    in0.ClosingUnitCost AS ClosingUnitCost,
    in0.ClosingLaborCost AS ClosingLaborCost,
    in0.ClosingPlannerCode AS ClosingPlannerCode,
    in0.PreviousQuarterUnitCost AS PreviousQuarterUnitCost,
    in0.PreviousQuarterLaborCost AS PreviousQuarterLaborCost,
    in0.PreviousQuarterPlannerCode AS PreviousQuarterPlannerCode,
    in0.OpeningPlatingCost AS OpeningPlatingCost,
    in0.ClostingPlatingCost AS ClostingPlatingCost,
    in0.PreviousQuarterPlatingCost AS PreviousQuarterPlatingCost,
    in0.OpeningBuyer AS OpeningBuyer,
    in0.ClosingBuyer AS ClosingBuyer,
    in0.PreviousQuarterBuyer AS PreviousQuarterBuyer,
    in0.OpeningOrderPolicy AS OpeningOrderPolicy,
    in0.ClosingOrderPolicy AS ClosingOrderPolicy,
    in0.PreviousQuarterOrderPolicy AS PreviousQuarterOrderPolicy,
    (
      CASE
        WHEN (in0.OpeningPlannerCode = in1.PlannerCode)
          THEN in1.AccountingPlannerCode
        ELSE NULL
      END
    ) AS OpeningPlannerCode,
    (
      CASE
        WHEN (in0.OpeningPlannerCode = in1.PlannerCode)
          THEN in1.IsBOM
        ELSE NULL
      END
    ) AS OpeningIsBOM,
    in0.* EXCEPT (`PartNumber`, 
    `OpeningUnitCost`, 
    `OpeningLaborCost`, 
    `ClosingUnitCost`, 
    `ClosingLaborCost`, 
    `ClosingPlannerCode`, 
    `PreviousQuarterUnitCost`, 
    `PreviousQuarterLaborCost`, 
    `PreviousQuarterPlannerCode`, 
    `OpeningPlatingCost`, 
    `ClostingPlatingCost`, 
    `PreviousQuarterPlatingCost`, 
    `OpeningBuyer`, 
    `ClosingBuyer`, 
    `PreviousQuarterBuyer`, 
    `OpeningOrderPolicy`, 
    `ClosingOrderPolicy`, 
    `PreviousQuarterOrderPolicy`, 
    `OpeningPlannerCode`),
    in1.* EXCEPT (`PlannerCode`, `IsBOM`, `AccountingPlannerCode`)
  
  FROM Cleanse_123 AS in0
  LEFT JOIN AlteryxSelect_72 AS in1
     ON (in0.OpeningPlannerCode = in1.PlannerCode)

),

Join_129_left_UnionLeftOuter AS (

  SELECT 
    in0.PartNumber AS PartNumber,
    in0.OpeningUnitCost AS OpeningUnitCost,
    in0.OpeningLaborCost AS OpeningLaborCost,
    in0.OpeningIsBOM AS OpeningIsBOM,
    in0.OpeningPlannerCode AS OpeningPlannerCode,
    in0.ClosingUnitCost AS ClosingUnitCost,
    in0.ClosingLaborCost AS ClosingLaborCost,
    in0.PreviousQuarterUnitCost AS PreviousQuarterUnitCost,
    in0.PreviousQuarterLaborCost AS PreviousQuarterLaborCost,
    in0.PreviousQuarterPlannerCode AS PreviousQuarterPlannerCode,
    in0.OpeningPlatingCost AS OpeningPlatingCost,
    in0.ClostingPlatingCost AS ClostingPlatingCost,
    in0.PreviousQuarterPlatingCost AS PreviousQuarterPlatingCost,
    in0.OpeningBuyer AS OpeningBuyer,
    in0.ClosingBuyer AS ClosingBuyer,
    in0.PreviousQuarterBuyer AS PreviousQuarterBuyer,
    in0.OpeningOrderPolicy AS OpeningOrderPolicy,
    in0.ClosingOrderPolicy AS ClosingOrderPolicy,
    in0.PreviousQuarterOrderPolicy AS PreviousQuarterOrderPolicy,
    (
      CASE
        WHEN (in0.ClosingPlannerCode = in1.PlannerCode)
          THEN in1.AccountingPlannerCode
        ELSE NULL
      END
    ) AS ClosingPlannerCode,
    (
      CASE
        WHEN (in0.ClosingPlannerCode = in1.PlannerCode)
          THEN in1.IsBOM
        ELSE NULL
      END
    ) AS ClosingIsBOM,
    in0.* EXCEPT (`PartNumber`, 
    `OpeningUnitCost`, 
    `OpeningLaborCost`, 
    `OpeningIsBOM`, 
    `OpeningPlannerCode`, 
    `ClosingUnitCost`, 
    `ClosingLaborCost`, 
    `PreviousQuarterUnitCost`, 
    `PreviousQuarterLaborCost`, 
    `PreviousQuarterPlannerCode`, 
    `OpeningPlatingCost`, 
    `ClostingPlatingCost`, 
    `PreviousQuarterPlatingCost`, 
    `OpeningBuyer`, 
    `ClosingBuyer`, 
    `PreviousQuarterBuyer`, 
    `OpeningOrderPolicy`, 
    `ClosingOrderPolicy`, 
    `PreviousQuarterOrderPolicy`, 
    `ClosingPlannerCode`),
    in1.* EXCEPT (`PlannerCode`, `IsBOM`, `AccountingPlannerCode`)
  
  FROM Join_127_left_UnionLeftOuter AS in0
  LEFT JOIN AlteryxSelect_72 AS in1
     ON (in0.ClosingPlannerCode = in1.PlannerCode)

),

Join_130_left_UnionLeftOuter AS (

  SELECT 
    in0.PartNumber AS PartNumber,
    in0.OpeningUnitCost AS OpeningUnitCost,
    in0.OpeningLaborCost AS OpeningLaborCost,
    in0.OpeningIsBOM AS OpeningIsBOM,
    in0.OpeningPlannerCode AS OpeningPlannerCode,
    in0.ClosingUnitCost AS ClosingUnitCost,
    in0.ClosingLaborCost AS ClosingLaborCost,
    in0.ClosingIsBOM AS ClosingIsBOM,
    in0.ClosingPlannerCode AS ClosingPlannerCode,
    in0.PreviousQuarterUnitCost AS PreviousQuarterUnitCost,
    in0.PreviousQuarterLaborCost AS PreviousQuarterLaborCost,
    in0.OpeningPlatingCost AS OpeningPlatingCost,
    in0.ClostingPlatingCost AS ClostingPlatingCost,
    in0.PreviousQuarterPlatingCost AS PreviousQuarterPlatingCost,
    in0.OpeningBuyer AS OpeningBuyer,
    in0.ClosingBuyer AS ClosingBuyer,
    in0.PreviousQuarterBuyer AS PreviousQuarterBuyer,
    in0.OpeningOrderPolicy AS OpeningOrderPolicy,
    in0.ClosingOrderPolicy AS ClosingOrderPolicy,
    in0.PreviousQuarterOrderPolicy AS PreviousQuarterOrderPolicy,
    (
      CASE
        WHEN (in0.PreviousQuarterPlannerCode = in1.PlannerCode)
          THEN in1.AccountingPlannerCode
        ELSE NULL
      END
    ) AS PreviousQuarterPlannerCode,
    (
      CASE
        WHEN (in0.PreviousQuarterPlannerCode = in1.PlannerCode)
          THEN in1.IsBOM
        ELSE NULL
      END
    ) AS PreviousQuarterIsBOM,
    in0.* EXCEPT (`PartNumber`, 
    `OpeningUnitCost`, 
    `OpeningLaborCost`, 
    `OpeningIsBOM`, 
    `OpeningPlannerCode`, 
    `ClosingUnitCost`, 
    `ClosingLaborCost`, 
    `ClosingIsBOM`, 
    `ClosingPlannerCode`, 
    `PreviousQuarterUnitCost`, 
    `PreviousQuarterLaborCost`, 
    `OpeningPlatingCost`, 
    `ClostingPlatingCost`, 
    `PreviousQuarterPlatingCost`, 
    `OpeningBuyer`, 
    `ClosingBuyer`, 
    `PreviousQuarterBuyer`, 
    `OpeningOrderPolicy`, 
    `ClosingOrderPolicy`, 
    `PreviousQuarterOrderPolicy`, 
    `PreviousQuarterPlannerCode`),
    in1.* EXCEPT (`PlannerCode`, `IsBOM`, `AccountingPlannerCode`)
  
  FROM Join_129_left_UnionLeftOuter AS in0
  LEFT JOIN AlteryxSelect_72 AS in1
     ON (in0.PreviousQuarterPlannerCode = in1.PlannerCode)

),

AlteryxSelect_118 AS (

  SELECT 
    PartNumber AS PartNumber,
    OpeningUnitCost AS OpeningUnitCost,
    ClosingUnitCost AS ClosingUnitCost,
    PreviousQuarterUnitCost AS PreviousQuarterUnitCost,
    OpeningLaborCost AS OpeningLaborCost,
    ClosingLaborCost AS ClosingLaborCost,
    PreviousQuarterLaborCost AS PreviousQuarterLaborCost,
    OpeningPlatingCost AS OpeningPlatingCost,
    ClostingPlatingCost AS ClostingPlatingCost,
    PreviousQuarterPlatingCost AS PreviousQuarterPlatingCost,
    OpeningPlannerCode AS OpeningPlannerCode,
    ClosingPlannerCode AS ClosingPlannerCode,
    PreviousQuarterPlannerCode AS PreviousQuarterPlannerCode,
    OpeningIsBOM AS OpeningIsBOM,
    ClosingIsBOM AS ClosingIsBOM,
    PreviousQuarterIsBOM AS PreviousQuarterIsBOM,
    OpeningBuyer AS OpeningBuyer,
    ClosingBuyer AS ClosingBuyer,
    PreviousQuarterBuyer AS PreviousQuarterBuyer,
    OpeningOrderPolicy AS OpeningOrderPolicy,
    ClosingOrderPolicy AS ClosingOrderPolicy,
    PreviousQuarterOrderPolicy AS PreviousQuarterOrderPolicy,
    OpeningCustomerSupplied AS OpeningCustomerSupplied,
    ClosingCustomerSupplied AS ClosingCustomerSupplied,
    PreviousQuarterCustomerSupplied AS PreviousQuarterCustomerSupplied,
    OpeningStandardUnitCost AS OpeningStandardUnitCost,
    OpeningStandardLaborCost AS OpeningStandardLaborCost,
    OpeningStandardPlatingCost AS OpeningStandardPlatingCost,
    ClosingStandardUnitCost AS ClosingStandardUnitCost,
    ClosingStandardLaborCost AS ClosingStandardLaborCost,
    ClosingStandardPlatingCost AS ClosingStandardPlatingCost,
    PreviousQuarterStandardUnitCost AS PreviousQuarterStandardUnitCost,
    PreviousQuarterStandardLaborCost AS PreviousQuarterStandardLaborCost,
    PreviousQuarterStandardPlatingCost AS PreviousQuarterStandardPlatingCost,
    * EXCEPT (`PartNumber`, 
    `OpeningUnitCost`, 
    `ClosingUnitCost`, 
    `PreviousQuarterUnitCost`, 
    `OpeningLaborCost`, 
    `ClosingLaborCost`, 
    `PreviousQuarterLaborCost`, 
    `OpeningPlatingCost`, 
    `ClostingPlatingCost`, 
    `PreviousQuarterPlatingCost`, 
    `OpeningPlannerCode`, 
    `ClosingPlannerCode`, 
    `PreviousQuarterPlannerCode`, 
    `OpeningIsBOM`, 
    `ClosingIsBOM`, 
    `PreviousQuarterIsBOM`, 
    `OpeningBuyer`, 
    `ClosingBuyer`, 
    `PreviousQuarterBuyer`, 
    `OpeningOrderPolicy`, 
    `ClosingOrderPolicy`, 
    `PreviousQuarterOrderPolicy`, 
    `OpeningCustomerSupplied`, 
    `ClosingCustomerSupplied`, 
    `PreviousQuarterCustomerSupplied`, 
    `OpeningStandardUnitCost`, 
    `OpeningStandardLaborCost`, 
    `OpeningStandardPlatingCost`, 
    `ClosingStandardUnitCost`, 
    `ClosingStandardLaborCost`, 
    `ClosingStandardPlatingCost`, 
    `PreviousQuarterStandardUnitCost`, 
    `PreviousQuarterStandardLaborCost`, 
    `PreviousQuarterStandardPlatingCost`)
  
  FROM Join_130_left_UnionLeftOuter AS in0

),

Join_122_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`PartNumber`)
  
  FROM Formula_91_1 AS in0
  LEFT JOIN AlteryxSelect_118 AS in1
     ON (in0.PartNumber = in1.PartNumber)

),

AlteryxSelect_74 AS (

  SELECT 
    PartNumber AS PartNumber,
    ReportingSeries AS Series,
    ReportingSeriesProductEra AS Era,
    ReportingSeriesProductWeave AS Weave,
    ReportingSeriesProductSolutionBlock AS Block,
    ReportingSeriesProductSegment AS Segment,
    ItemType AS ItemType,
    PCMfgItemType AS PCMfgItemType
  
  FROM AlteryxSelect_182 AS in0

),

Join_138_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`PartNumber`)
  
  FROM Join_122_left_UnionLeftOuter AS in0
  INNER JOIN AlteryxSelect_74 AS in1
     ON (in0.PartNumber = in1.PartNumber)

),

Formula_134_0 AS (

  SELECT 
    CAST((OpeningBalance * ClosingUnitCost) AS DOUBLE) AS OpeningValue,
    CAST((ClosingBalance * ClosingUnitCost) AS DOUBLE) AS ClosingValue,
    *
  
  FROM Join_138_inner AS in0

)

SELECT *

FROM Formula_134_0
