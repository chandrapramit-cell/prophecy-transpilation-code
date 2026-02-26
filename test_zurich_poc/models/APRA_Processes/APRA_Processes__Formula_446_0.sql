{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH DynamicInput_39 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_39') }}

),

DynamicInput_464 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_464') }}

),

Formula_99_to_Formula_183_2 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_99_to_Formula_183_2')}}

),

Filter_146 AS (

  SELECT * 
  
  FROM Formula_99_to_Formula_183_2 AS in0
  
  WHERE (((v_TZAC = 'Unallocated') OR (v_TZAC IS NULL)) OR ((LENGTH(v_TZAC)) = 0))

),

Join_145_inner AS (

  SELECT 
    in0.* EXCEPT (`Share percent`),
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `CRR`, 
    `Deemed`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`)
  
  FROM Filter_146 AS in0
  INNER JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Formula_460_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((`TZAC Code` IS NULL) OR ((LENGTH(`TZAC Code`)) = 0))
          THEN `Manual TZAC Map`
        ELSE `TZAC Code`
      END
    ) AS STRING) AS v_TZAC,
    * EXCEPT (`v_tzac`)
  
  FROM Join_145_inner AS in0

),

AlteryxSelect_148 AS (

  SELECT * EXCEPT (`TZAC Code`, `Manual TZAC Map`)
  
  FROM Formula_460_0 AS in0

),

Filter_146_reject AS (

  SELECT * 
  
  FROM Formula_99_to_Formula_183_2 AS in0
  
  WHERE (
          (NOT(((v_TZAC = 'Unallocated') OR (v_TZAC IS NULL)) OR ((LENGTH(v_TZAC)) = 0)))
          OR ((((v_TZAC = 'Unallocated') OR (v_TZAC IS NULL)) OR ((LENGTH(v_TZAC)) = 0)) IS NULL)
        )

),

Join_145_left AS (

  SELECT in0.*
  
  FROM Filter_146 AS in0
  ANTI JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Union_462 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_145_left', 'AlteryxSelect_148', 'Filter_146_reject'], 
      [
        '[{"name": "vROLValue", "dataType": "Double"}, {"name": "v_Branch", "dataType": "String"}, {"name": "nBalOut0", "dataType": "Double"}, {"name": "nBalOut1", "dataType": "Double"}, {"name": "nBalOut2", "dataType": "Double"}, {"name": "nBalOut3", "dataType": "Double"}, {"name": "nTargetPeriod", "dataType": "Integer"}, {"name": "v_RiskNo", "dataType": "String"}, {"name": "v_RiskClass", "dataType": "String"}, {"name": "v_Period0", "dataType": "Decimal"}, {"name": "v_BalOut2", "dataType": "Decimal"}, {"name": "v_TZAC", "dataType": "String"}, {"name": "v_AccountName", "dataType": "String"}, {"name": "v_Policy", "dataType": "String"}, {"name": "v_ClaimNumber", "dataType": "String"}, {"name": "v_BalOut1", "dataType": "Decimal"}, {"name": "v_PremClass", "dataType": "String"}, {"name": "v_ClaimBranch", "dataType": "String"}, {"name": "v_Period1", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_Period3", "dataType": "Decimal"}, {"name": "v_Period2", "dataType": "Decimal"}, {"name": "v_BalOut0", "dataType": "Decimal"}, {"name": "v_Proportion", "dataType": "Decimal"}, {"name": "v_AccountBranch", "dataType": "String"}, {"name": "v_ProdClass", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "v_BalOut3", "dataType": "Decimal"}, {"name": "v_PolBranch", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "Share percent", "dataType": "Double"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "vROLValue", "dataType": "Double"}, {"name": "v_Branch", "dataType": "String"}, {"name": "nBalOut0", "dataType": "Double"}, {"name": "nBalOut1", "dataType": "Double"}, {"name": "nBalOut2", "dataType": "Double"}, {"name": "nBalOut3", "dataType": "Double"}, {"name": "nTargetPeriod", "dataType": "Integer"}, {"name": "v_RiskNo", "dataType": "String"}, {"name": "v_RiskClass", "dataType": "String"}, {"name": "v_Period0", "dataType": "Decimal"}, {"name": "v_BalOut2", "dataType": "Decimal"}, {"name": "v_AccountName", "dataType": "String"}, {"name": "v_Policy", "dataType": "String"}, {"name": "v_ClaimNumber", "dataType": "String"}, {"name": "v_BalOut1", "dataType": "Decimal"}, {"name": "v_PremClass", "dataType": "String"}, {"name": "v_ClaimBranch", "dataType": "String"}, {"name": "v_Period1", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_Period3", "dataType": "Decimal"}, {"name": "v_Period2", "dataType": "Decimal"}, {"name": "v_BalOut0", "dataType": "Decimal"}, {"name": "v_Proportion", "dataType": "Decimal"}, {"name": "v_AccountBranch", "dataType": "String"}, {"name": "v_ProdClass", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "v_BalOut3", "dataType": "Decimal"}, {"name": "v_PolBranch", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "Share percent", "dataType": "Double"}]', 
        '[{"name": "vROLValue", "dataType": "Double"}, {"name": "v_Branch", "dataType": "String"}, {"name": "nBalOut0", "dataType": "Double"}, {"name": "nBalOut1", "dataType": "Double"}, {"name": "nBalOut2", "dataType": "Double"}, {"name": "nBalOut3", "dataType": "Double"}, {"name": "nTargetPeriod", "dataType": "Integer"}, {"name": "v_RiskNo", "dataType": "String"}, {"name": "v_RiskClass", "dataType": "String"}, {"name": "v_Period0", "dataType": "Decimal"}, {"name": "v_BalOut2", "dataType": "Decimal"}, {"name": "v_TZAC", "dataType": "String"}, {"name": "v_AccountName", "dataType": "String"}, {"name": "v_Policy", "dataType": "String"}, {"name": "v_ClaimNumber", "dataType": "String"}, {"name": "v_BalOut1", "dataType": "Decimal"}, {"name": "v_PremClass", "dataType": "String"}, {"name": "v_ClaimBranch", "dataType": "String"}, {"name": "v_Period1", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_Period3", "dataType": "Decimal"}, {"name": "v_Period2", "dataType": "Decimal"}, {"name": "v_BalOut0", "dataType": "Decimal"}, {"name": "v_Proportion", "dataType": "Decimal"}, {"name": "v_AccountBranch", "dataType": "String"}, {"name": "v_ProdClass", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "v_BalOut3", "dataType": "Decimal"}, {"name": "v_PolBranch", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "Share percent", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_130_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`TZAC`, `variableDesc`, `Name of Counterparty`, `variableKey`)
  
  FROM Union_462 AS in0
  LEFT JOIN DynamicInput_39 AS in1
     ON (in0.v_TZAC = in1.TZAC)

),

Formula_182_0 AS (

  SELECT 
    CAST('Blank' AS STRING) AS v_DebtorCategory,
    CAST(NULL AS STRING) AS v_UnderwritingYear,
    *
  
  FROM Join_130_left_UnionLeftOuter AS in0

),

Join_180_left_UnionLeftOuter AS (

  SELECT 
    in0.* EXCEPT (`Share percent`),
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `TZAC Code`, 
    `CRR`, 
    `Manual TZAC Map`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`)
  
  FROM Formula_182_0 AS in0
  LEFT JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Formula_121_to_Formula_117_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (CAST((SUBSTRING(v_AccountType, 1, 1)) AS STRING) = 'T')
          THEN 'Treaty'
        WHEN (CAST((SUBSTRING(v_AccountType, 1, 1)) AS STRING) = 'O')
          THEN 'FAC - External'
        ELSE 'Not RI Acct'
      END
    ) AS STRING) AS v_RIType,
    CAST((
      CASE
        WHEN CAST((`Trading Partner` IN ('111111', '222222')) AS BOOLEAN)
          THEN 'FAC - IPS'
        ELSE 'Blank'
      END
    ) AS STRING) AS v_RITypeIPS,
    CAST(NULL AS STRING) AS `Debtors Description`,
    CAST(NULL AS STRING) AS variableType,
    *
  
  FROM Join_180_left_UnionLeftOuter AS in0

),

Formula_249_to_Formula_258_2 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_249_to_Formula_258_2')}}

),

Formula_261_to_Formula_265_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (v_TZAC = '')
          THEN 'Unallocated'
        ELSE v_TZAC
      END
    ) AS STRING) AS v_TZAC,
    CAST(v_RIBranch AS STRING) AS v_Branch,
    CAST((SUBSTRING(CAST(pPeriod AS STRING), 1, 4)) AS STRING) AS v_AccidentYear,
    CAST('None' AS STRING) AS v_ProductCode,
    CAST('None' AS STRING) AS v_PremiumClass,
    * EXCEPT (`v_tzac`)
  
  FROM Formula_249_to_Formula_258_2 AS in0

),

Filter_228 AS (

  SELECT * 
  
  FROM Formula_261_to_Formula_265_0 AS in0
  
  WHERE (v_TZAC = 'Unallocated')

),

Join_227_inner AS (

  SELECT 
    in0.* EXCEPT (`Share percent`),
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `CRR`, 
    `Deemed`, 
    `Manual TZAC Map`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`)
  
  FROM Filter_228 AS in0
  INNER JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

AlteryxSelect_230 AS (

  SELECT 
    `TZAC Code` AS v_TZAC,
    * EXCEPT (`v_TZAC`, `TZAC Code`)
  
  FROM Join_227_inner AS in0

),

Filter_454_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_230 AS in0
  
  WHERE (
          (
            NOT(
              (LENGTH(v_TZAC)) = 0)
          ) OR (((LENGTH(v_TZAC)) = 0) IS NULL)
        )

),

Filter_454 AS (

  SELECT * 
  
  FROM AlteryxSelect_230 AS in0
  
  WHERE ((LENGTH(v_TZAC)) = 0)

),

Filter_228_reject AS (

  SELECT * 
  
  FROM Formula_261_to_Formula_265_0 AS in0
  
  WHERE (
          (
            NOT(
              v_TZAC = 'Unallocated')
          ) OR ((v_TZAC = 'Unallocated') IS NULL)
        )

),

Union_458 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_228_reject', 'Filter_454'], 
      [
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "n_DaysOS", "dataType": "Double"}, {"name": "v_Debtors6Months", "dataType": "Double"}, {"name": "sDate", "dataType": "Date"}, {"name": "n_6MonthDay", "dataType": "Date"}, {"name": "sDateCalc", "dataType": "String"}, {"name": "v_DebtorsDesc", "dataType": "String"}, {"name": "Right_Share percent", "dataType": "Double"}, {"name": "v_EndOfMonth", "dataType": "String"}, {"name": "v_DateCalc", "dataType": "Decimal"}, {"name": "v_DateEffect", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_RIBranch", "dataType": "String"}, {"name": "v_TransType", "dataType": "String"}, {"name": "v_Type", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "Share percent", "dataType": "Double"}, {"name": "v_OSDebtors", "dataType": "Decimal"}, {"name": "v_STMTID", "dataType": "Integer"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "n_DaysOS", "dataType": "Double"}, {"name": "v_Debtors6Months", "dataType": "Double"}, {"name": "sDate", "dataType": "Date"}, {"name": "n_6MonthDay", "dataType": "Date"}, {"name": "sDateCalc", "dataType": "String"}, {"name": "v_DebtorsDesc", "dataType": "String"}, {"name": "Right_Share percent", "dataType": "Double"}, {"name": "v_EndOfMonth", "dataType": "String"}, {"name": "v_DateCalc", "dataType": "Decimal"}, {"name": "v_DateEffect", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_RIBranch", "dataType": "String"}, {"name": "v_TransType", "dataType": "String"}, {"name": "v_Type", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "v_OSDebtors", "dataType": "Decimal"}, {"name": "v_STMTID", "dataType": "Integer"}, {"name": "Share percent", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_115 AS (

  SELECT 
    CAST(pPeriod AS STRING) AS Period,
    v_RITypeIPS AS `RI Type IPS`,
    v_RIType AS `RI Type`,
    v_Company AS Company,
    v_Branch AS Branch,
    v_Reinsurer AS Reinsurer,
    v_AccountType AS `Account Type`,
    v_AccidentYear AS `Accident Year`,
    v_TZAC AS TZAC,
    v_ProdClass AS `Product Code`,
    v_PremClass AS `Premium Class`,
    `Trading Partner` AS `Trading Partner`,
    Rating AS Rating,
    Resident AS Resident,
    v_DebtorCategory AS `Debtor Category`,
    Deemed AS Deemed,
    Authorised AS Authorised,
    `Debtors Description` AS `Debtors Description`,
    `Counter Party` AS Counterparty,
    variableType AS variableType,
    Docs AS Docs,
    `Govn Law` AS `Govn Law`,
    `APRA Identifier` AS `APRA Identifier`,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    `Group Name` AS `Group Name`,
    `Group Domicile` AS `Group Domicile`,
    Country AS Country,
    vROLValue AS `ROL Amount`,
    CAST(NULL AS STRING) AS `RWP Amount`
  
  FROM Formula_121_to_Formula_117_0 AS in0

),

Formula_418_0 AS (

  SELECT 
    CAST(NULL AS DOUBLE) AS `Debtors > 6 months`,
    CAST(NULL AS DOUBLE) AS Debtors,
    CAST(NULL AS STRING) AS `RWP Amount`,
    CAST(NULL AS DOUBLE) AS `RUP Amount`,
    CAST(NULL AS DOUBLE) AS `Days OS`,
    * EXCEPT (`rwp amount`)
  
  FROM AlteryxSelect_115 AS in0

),

Filter_631 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Filter_631')}}

),

Join_358_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.v_Reinsurer = in1.Reinsurer)
          THEN in1.`TZAC Code`
        ELSE NULL
      END
    ) AS v_TZAC,
    (
      CASE
        WHEN (in0.v_Reinsurer = in1.Reinsurer)
          THEN in1.`Account Type`
        ELSE NULL
      END
    ) AS v_AccountType,
    (
      CASE
        WHEN (in0.v_Reinsurer = in1.Reinsurer)
          THEN in1.`Account Branch`
        ELSE NULL
      END
    ) AS v_Branch,
    in0.*,
    in1.* EXCEPT (`Reinsurer`, 
    `Account Name`, 
    `CRR`, 
    `Deemed`, 
    `Manual TZAC Map`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`, 
    `Account Type`, 
    `Account Branch`, 
    `TZAC Code`)
  
  FROM Filter_631 AS in0
  LEFT JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Formula_371_0 AS (

  SELECT 
    CAST('None' AS STRING) AS v_AccidentYear,
    *
  
  FROM Join_358_left_UnionLeftOuter AS in0

),

Filter_420 AS (

  SELECT * 
  
  FROM Formula_418_0 AS in0
  
  WHERE (
          NOT(
            `ROL Amount` = 0)
        )

),

Formula_442_0 AS (

  SELECT 
    CAST('Cube_Update_APRA Output from APRA ROL' AS STRING) AS Source,
    *
  
  FROM Filter_420 AS in0

),

Union_417_reformat_0 AS (

  SELECT 
    `APRA Identifier` AS `APRA Identifier`,
    `Accident Year` AS `Accident Year`,
    `Account Type` AS `Account Type`,
    Authorised AS Authorised,
    Branch AS Branch,
    Company AS Company,
    Counterparty AS Counterparty,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    Country AS Country,
    `Days OS` AS `Days OS`,
    `Debtor Category` AS `Debtor Category`,
    CAST(Debtors AS DOUBLE) AS Debtors,
    `Debtors > 6 months` AS `Debtors > 6 months`,
    `Debtors Description` AS `Debtors Description`,
    Deemed AS Deemed,
    Docs AS Docs,
    `Govn Law` AS `Govn Law`,
    `Group Domicile` AS `Group Domicile`,
    `Group Name` AS `Group Name`,
    Period AS Period,
    `Premium Class` AS `Premium Class`,
    `Product Code` AS `Product Code`,
    `RI Type` AS `RI Type`,
    `RI Type IPS` AS `RI Type IPS`,
    `ROL Amount` AS `ROL Amount`,
    `RUP Amount` AS `RUP Amount`,
    CAST(`RWP Amount` AS STRING) AS `RWP Amount`,
    Rating AS Rating,
    Reinsurer AS Reinsurer,
    Resident AS Resident,
    Source AS Source,
    TZAC AS TZAC,
    `Trading Partner` AS `Trading Partner`,
    variableType AS variableType
  
  FROM Formula_442_0 AS in0

),

Filter_378_reject AS (

  SELECT * 
  
  FROM Formula_371_0 AS in0
  
  WHERE (
          (
            NOT(
              (LENGTH(v_TZAC)) = 0)
          ) OR (((LENGTH(v_TZAC)) = 0) IS NULL)
        )

),

Filter_378 AS (

  SELECT * 
  
  FROM Formula_371_0 AS in0
  
  WHERE ((LENGTH(v_TZAC)) = 0)

),

Join_373_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.v_Reinsurer = in1.Reinsurer)
          THEN in1.`Manual TZAC Map`
        ELSE NULL
      END
    ) AS v_TZAC,
    in0.* EXCEPT (`v_TZAC`, `Share percent`),
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `TZAC Code`, 
    `CRR`, 
    `Deemed`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`, 
    `Manual TZAC Map`)
  
  FROM Filter_378 AS in0
  LEFT JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Union_375 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_378_reject', 'Join_373_left_UnionLeftOuter'], 
      [
        '[{"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_TZAC", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_Period", "dataType": "String"}, {"name": "v_Company", "dataType": "String"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "RUP", "dataType": "Double"}, {"name": "RUA", "dataType": "Double"}, {"name": "Share percent", "dataType": "Double"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_Period", "dataType": "String"}, {"name": "v_Company", "dataType": "String"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "RUP", "dataType": "Double"}, {"name": "RUA", "dataType": "Double"}, {"name": "Share percent", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_300_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`TZAC`, `variableDesc`, `Name of Counterparty`, `variableKey`)
  
  FROM Union_375 AS in0
  LEFT JOIN DynamicInput_39 AS in1
     ON (in0.v_TZAC = in1.TZAC)

),

Formula_306_0 AS (

  SELECT 
    CAST('Blank' AS STRING) AS v_DebtorCategory,
    CAST(NULL AS STRING) AS v_UnderwritingYear,
    *
  
  FROM Join_300_left_UnionLeftOuter AS in0

),

Formula_8_to_Formula_23_1 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_8_to_Formula_23_1')}}

),

Filter_470 AS (

  SELECT * 
  
  FROM Formula_8_to_Formula_23_1 AS in0
  
  WHERE (((v_TZAC = 'Unallocated') OR (v_TZAC IS NULL)) OR ((LENGTH(v_TZAC)) = 0))

),

Join_465_left AS (

  SELECT in0.*
  
  FROM Filter_470 AS in0
  ANTI JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Join_465_inner AS (

  SELECT 
    in0.* EXCEPT (`Share percent`),
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `CRR`, 
    `Deemed`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`)
  
  FROM Filter_470 AS in0
  INNER JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Formula_467_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((`TZAC Code` IS NULL) OR ((LENGTH(`TZAC Code`)) = 0))
          THEN `Manual TZAC Map`
        ELSE `TZAC Code`
      END
    ) AS STRING) AS v_TZAC,
    * EXCEPT (`v_tzac`)
  
  FROM Join_465_inner AS in0

),

AlteryxSelect_466 AS (

  SELECT * EXCEPT (`TZAC Code`, `Manual TZAC Map`)
  
  FROM Formula_467_0 AS in0

),

Filter_470_reject AS (

  SELECT * 
  
  FROM Formula_8_to_Formula_23_1 AS in0
  
  WHERE (
          (NOT(((v_TZAC = 'Unallocated') OR (v_TZAC IS NULL)) OR ((LENGTH(v_TZAC)) = 0)))
          OR ((((v_TZAC = 'Unallocated') OR (v_TZAC IS NULL)) OR ((LENGTH(v_TZAC)) = 0)) IS NULL)
        )

),

Union_469 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_465_left', 'AlteryxSelect_466', 'Filter_470_reject'], 
      [
        '[{"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_LineNo", "dataType": "String"}, {"name": "vPeriod", "dataType": "String"}, {"name": "vVersion", "dataType": "String"}, {"name": "v_TZAC", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_RiskNo", "dataType": "Decimal"}, {"name": "v_RiskClass", "dataType": "String"}, {"name": "v_AccountingYear", "dataType": "String"}, {"name": "v_RIName", "dataType": "String"}, {"name": "v_Policy", "dataType": "String"}, {"name": "v_PremClass", "dataType": "String"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_AccountBranch", "dataType": "String"}, {"name": "v_ProdClass", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "v_PolBranch", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_AccountingMonth", "dataType": "String"}, {"name": "v_Company", "dataType": "String"}, {"name": "Share percent", "dataType": "Double"}, {"name": "v_RWP", "dataType": "Decimal"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_LineNo", "dataType": "String"}, {"name": "vPeriod", "dataType": "String"}, {"name": "vVersion", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_RiskNo", "dataType": "Decimal"}, {"name": "v_RiskClass", "dataType": "String"}, {"name": "v_AccountingYear", "dataType": "String"}, {"name": "v_RIName", "dataType": "String"}, {"name": "v_Policy", "dataType": "String"}, {"name": "v_PremClass", "dataType": "String"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_AccountBranch", "dataType": "String"}, {"name": "v_ProdClass", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "v_PolBranch", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_AccountingMonth", "dataType": "String"}, {"name": "v_Company", "dataType": "String"}, {"name": "v_RWP", "dataType": "Decimal"}, {"name": "Share percent", "dataType": "Double"}]', 
        '[{"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_LineNo", "dataType": "String"}, {"name": "vPeriod", "dataType": "String"}, {"name": "vVersion", "dataType": "String"}, {"name": "v_TZAC", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_RiskNo", "dataType": "Decimal"}, {"name": "v_RiskClass", "dataType": "String"}, {"name": "v_AccountingYear", "dataType": "String"}, {"name": "v_RIName", "dataType": "String"}, {"name": "v_Policy", "dataType": "String"}, {"name": "v_PremClass", "dataType": "String"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_AccountBranch", "dataType": "String"}, {"name": "v_ProdClass", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "v_PolBranch", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_AccountingMonth", "dataType": "String"}, {"name": "v_Company", "dataType": "String"}, {"name": "Share percent", "dataType": "Double"}, {"name": "v_RWP", "dataType": "Decimal"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_38_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`TZAC`, 
    `variableDesc`, 
    `Counter Party`, 
    `Authorised`, 
    `Docs`, 
    `Govn Law`, 
    `APRA Identifier`, 
    `Name of Counterparty`, 
    `Counterparty Domicile`, 
    `Group Name`, 
    `Group Domicile`, 
    `Country`, 
    `variableKey`)
  
  FROM Union_469 AS in0
  LEFT JOIN DynamicInput_39 AS in1
     ON (in0.v_TZAC = in1.TZAC)

),

Formula_46_0 AS (

  SELECT 
    CAST('Blank' AS STRING) AS v_DebtorCategory,
    *
  
  FROM Join_38_left_UnionLeftOuter AS in0

),

Join_51_left_UnionLeftOuter AS (

  SELECT 
    in0.* EXCEPT (`Share percent`),
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `TZAC Code`, 
    `CRR`, 
    `Manual TZAC Map`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`)
  
  FROM Formula_46_0 AS in0
  LEFT JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Join_58_left_UnionLeftOuter AS (

  SELECT 
    in0.v_RIName AS v_RIName,
    in0.`Business Category` AS `Business Category`,
    in0.vPeriod AS vPeriod,
    in0.pPeriod AS pPeriod,
    in0.v_Policy AS v_Policy,
    in0.`Occupation Type` AS `Occupation Type`,
    in0.Effective AS Effective,
    in0.v_TZAC AS v_TZAC,
    in0.Resident AS Resident,
    in0.v_AccountingYear AS v_AccountingYear,
    in0.v_PolBranch AS v_PolBranch,
    in0.v_PremClass AS v_PremClass,
    in0.v_RiskClass AS v_RiskClass,
    in0.vVersion AS vVersion,
    in0.v_AccountingMonth AS v_AccountingMonth,
    in0.v_AccidentYear AS v_AccidentYear,
    in0.Related AS Related,
    in0.Rating AS Rating,
    in0.`Co-Ins` AS `Co-Ins`,
    in0.v_RiskNo AS v_RiskNo,
    in0.Valid AS Valid,
    in0.v_Company AS v_Company,
    in0.Deemed AS Deemed,
    in0.`Share percent` AS `Share percent`,
    in0.v_DebtorCategory AS v_DebtorCategory,
    in0.v_AccountBranch AS v_AccountBranch,
    in0.v_AccountType AS v_AccountType,
    in0.v_ProdClass AS v_ProdClass,
    in1.Authorised AS Authorised,
    in0.`APRA Grade` AS `APRA Grade`,
    in0.v_RWP AS v_RWP,
    in0.v_Branch AS v_Branch,
    in0.`Trading Partner` AS `Trading Partner`,
    in0.v_Reinsurer AS v_Reinsurer,
    in0.v_LineNo AS v_LineNo
  
  FROM Join_51_left_UnionLeftOuter AS in0
  LEFT JOIN DynamicInput_39 AS in1
     ON (in0.v_TZAC = in1.TZAC)

),

Formula_60_0 AS (

  SELECT 
    CAST(NULL AS STRING) AS v_UnderwritingYear,
    *
  
  FROM Join_58_left_UnionLeftOuter AS in0

),

Filter_229 AS (

  SELECT * 
  
  FROM Union_458 AS in0
  
  WHERE ((LENGTH(v_TZAC)) = 0)

),

Join_233_inner AS (

  SELECT 
    in0.* EXCEPT (`Share percent`),
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `TZAC Code`, 
    `CRR`, 
    `Deemed`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`)
  
  FROM Filter_229 AS in0
  INNER JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

AlteryxSelect_236 AS (

  SELECT 
    `Manual TZAC Map` AS v_TZAC,
    * EXCEPT (`v_TZAC`, `Manual TZAC Map`)
  
  FROM Join_233_inner AS in0

),

Join_66_left_UnionLeftOuter AS (

  SELECT 
    in0.* EXCEPT (`Business Category`, `Occupation Type`, `APRA Grade`, `Effective`, `Valid`, `Related`, `Co-Ins`),
    in1.* EXCEPT (`TZAC`, 
    `variableDesc`, 
    `Trading Partner`, 
    `Rating`, 
    `Resident`, 
    `Authorised`, 
    `Docs`, 
    `Govn Law`, 
    `APRA Identifier`, 
    `Name of Counterparty`, 
    `Counterparty Domicile`, 
    `Group Name`, 
    `Group Domicile`, 
    `Country`, 
    `variableKey`)
  
  FROM Formula_60_0 AS in0
  LEFT JOIN DynamicInput_39 AS in1
     ON (in0.v_TZAC = in1.TZAC)

),

Join_70_left_UnionLeftOuter AS (

  SELECT 
    in0.v_RIName AS v_RIName,
    in0.`Business Category` AS `Business Category`,
    in0.vPeriod AS vPeriod,
    in0.`Counter Party` AS `Counter Party`,
    in0.v_Policy AS v_Policy,
    in0.`Occupation Type` AS `Occupation Type`,
    in0.Effective AS Effective,
    in0.v_TZAC AS v_TZAC,
    in0.Resident AS Resident,
    in0.v_AccountingYear AS v_AccountingYear,
    in0.v_UnderwritingYear AS v_UnderwritingYear,
    in0.v_PolBranch AS v_PolBranch,
    in0.v_PremClass AS v_PremClass,
    in0.v_RiskClass AS v_RiskClass,
    in0.vVersion AS vVersion,
    in0.v_AccountingMonth AS v_AccountingMonth,
    in0.v_AccidentYear AS v_AccidentYear,
    in0.Related AS Related,
    in0.Rating AS Rating,
    in0.`Co-Ins` AS `Co-Ins`,
    in0.v_RiskNo AS v_RiskNo,
    in0.Valid AS Valid,
    in0.v_Company AS v_Company,
    in0.Deemed AS Deemed,
    in0.v_DebtorCategory AS v_DebtorCategory,
    in0.v_AccountBranch AS v_AccountBranch,
    in0.v_AccountType AS v_AccountType,
    in0.v_ProdClass AS v_ProdClass,
    in0.Authorised AS Authorised,
    in0.`APRA Grade` AS `APRA Grade`,
    in0.v_RWP AS v_RWP,
    in0.v_Branch AS v_Branch,
    in0.`Trading Partner` AS `Trading Partner`,
    in0.v_Reinsurer AS v_Reinsurer,
    in0.v_LineNo AS v_LineNo,
    (
      CASE
        WHEN (in0.v_TZAC = in1.TZAC)
          THEN NULL
        ELSE in0.pPeriod
      END
    ) AS pPeriod,
    (
      CASE
        WHEN (in0.v_TZAC = in1.TZAC)
          THEN NULL
        ELSE in0.`Share percent`
      END
    ) AS `Share percent`
  
  FROM Join_66_left_UnionLeftOuter AS in0
  LEFT JOIN DynamicInput_39 AS in1
     ON (in0.v_TZAC = in1.TZAC)

),

Formula_73_to_Formula_87_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (CAST((SUBSTRING(v_AccountType, 1, 1)) AS STRING) = 'T')
          THEN 'Treaty'
        WHEN (CAST((SUBSTRING(v_AccountType, 1, 1)) AS STRING) = 'O')
          THEN 'FAC - External'
        ELSE 'Not RI Acct'
      END
    ) AS STRING) AS v_RIType,
    CAST((
      CASE
        WHEN CAST((`Trading Partner` IN ('111111', '222222')) AS BOOLEAN)
          THEN 'FAC - IPS'
        ELSE 'Blank'
      END
    ) AS STRING) AS v_RITypeIPS,
    CAST(NULL AS STRING) AS `Debtors Description`,
    CAST(NULL AS STRING) AS variableType,
    *
  
  FROM Join_70_left_UnionLeftOuter AS in0

),

Join_233_left AS (

  SELECT in0.*
  
  FROM Filter_229 AS in0
  ANTI JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Filter_229_reject AS (

  SELECT * 
  
  FROM Union_458 AS in0
  
  WHERE (
          (
            NOT(
              (LENGTH(v_TZAC)) = 0)
          ) OR (((LENGTH(v_TZAC)) = 0) IS NULL)
        )

),

Join_304_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.v_Reinsurer = in1.Reinsurer)
          THEN in1.`Share percent`
        ELSE NULL
      END
    ) AS `Right_Share percent`,
    in0.*,
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `TZAC Code`, 
    `CRR`, 
    `Manual TZAC Map`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`, 
    `Share percent`)
  
  FROM Formula_306_0 AS in0
  LEFT JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Join_227_left AS (

  SELECT in0.*
  
  FROM Filter_228 AS in0
  ANTI JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Union_235 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_227_left', 'Filter_229_reject', 'AlteryxSelect_236', 'Filter_454_reject', 'Join_233_left'], 
      [
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "n_DaysOS", "dataType": "Double"}, {"name": "v_Debtors6Months", "dataType": "Double"}, {"name": "sDate", "dataType": "Date"}, {"name": "n_6MonthDay", "dataType": "Date"}, {"name": "sDateCalc", "dataType": "String"}, {"name": "v_DebtorsDesc", "dataType": "String"}, {"name": "Right_Share percent", "dataType": "Double"}, {"name": "v_EndOfMonth", "dataType": "String"}, {"name": "v_DateCalc", "dataType": "Decimal"}, {"name": "v_DateEffect", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_RIBranch", "dataType": "String"}, {"name": "v_TransType", "dataType": "String"}, {"name": "v_Type", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "Share percent", "dataType": "Double"}, {"name": "v_OSDebtors", "dataType": "Decimal"}, {"name": "v_STMTID", "dataType": "Integer"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "n_DaysOS", "dataType": "Double"}, {"name": "v_Debtors6Months", "dataType": "Double"}, {"name": "sDate", "dataType": "Date"}, {"name": "n_6MonthDay", "dataType": "Date"}, {"name": "sDateCalc", "dataType": "String"}, {"name": "v_DebtorsDesc", "dataType": "String"}, {"name": "Right_Share percent", "dataType": "Double"}, {"name": "v_EndOfMonth", "dataType": "String"}, {"name": "v_DateCalc", "dataType": "Decimal"}, {"name": "v_DateEffect", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_RIBranch", "dataType": "String"}, {"name": "v_TransType", "dataType": "String"}, {"name": "v_Type", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "Share percent", "dataType": "Double"}, {"name": "v_OSDebtors", "dataType": "Decimal"}, {"name": "v_STMTID", "dataType": "Integer"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "n_DaysOS", "dataType": "Double"}, {"name": "v_Debtors6Months", "dataType": "Double"}, {"name": "sDate", "dataType": "Date"}, {"name": "n_6MonthDay", "dataType": "Date"}, {"name": "sDateCalc", "dataType": "String"}, {"name": "v_DebtorsDesc", "dataType": "String"}, {"name": "Right_Share percent", "dataType": "Double"}, {"name": "v_EndOfMonth", "dataType": "String"}, {"name": "v_DateCalc", "dataType": "Decimal"}, {"name": "v_DateEffect", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_RIBranch", "dataType": "String"}, {"name": "v_TransType", "dataType": "String"}, {"name": "v_Type", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "v_OSDebtors", "dataType": "Decimal"}, {"name": "v_STMTID", "dataType": "Integer"}, {"name": "Share percent", "dataType": "Double"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "n_DaysOS", "dataType": "Double"}, {"name": "v_Debtors6Months", "dataType": "Double"}, {"name": "sDate", "dataType": "Date"}, {"name": "n_6MonthDay", "dataType": "Date"}, {"name": "sDateCalc", "dataType": "String"}, {"name": "v_DebtorsDesc", "dataType": "String"}, {"name": "Right_Share percent", "dataType": "Double"}, {"name": "v_EndOfMonth", "dataType": "String"}, {"name": "v_DateCalc", "dataType": "Decimal"}, {"name": "v_DateEffect", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_RIBranch", "dataType": "String"}, {"name": "v_TransType", "dataType": "String"}, {"name": "v_Type", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "v_OSDebtors", "dataType": "Decimal"}, {"name": "v_STMTID", "dataType": "Integer"}, {"name": "Share percent", "dataType": "Double"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "v_Branch", "dataType": "String"}, {"name": "v_AccidentYear", "dataType": "String"}, {"name": "v_ProductCode", "dataType": "String"}, {"name": "v_PremiumClass", "dataType": "String"}, {"name": "n_DaysOS", "dataType": "Double"}, {"name": "v_Debtors6Months", "dataType": "Double"}, {"name": "sDate", "dataType": "Date"}, {"name": "n_6MonthDay", "dataType": "Date"}, {"name": "sDateCalc", "dataType": "String"}, {"name": "v_DebtorsDesc", "dataType": "String"}, {"name": "Right_Share percent", "dataType": "Double"}, {"name": "v_EndOfMonth", "dataType": "String"}, {"name": "v_DateCalc", "dataType": "Decimal"}, {"name": "v_DateEffect", "dataType": "Decimal"}, {"name": "v_Reinsurer", "dataType": "String"}, {"name": "v_RIBranch", "dataType": "String"}, {"name": "v_TransType", "dataType": "String"}, {"name": "v_Type", "dataType": "String"}, {"name": "v_AccountType", "dataType": "String"}, {"name": "pPeriod", "dataType": "Integer"}, {"name": "v_Company", "dataType": "String"}, {"name": "Share percent", "dataType": "Double"}, {"name": "v_OSDebtors", "dataType": "Decimal"}, {"name": "v_STMTID", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_217_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`TZAC`, `variableDesc`, `Name of Counterparty`, `variableKey`)
  
  FROM Union_235 AS in0
  LEFT JOIN DynamicInput_39 AS in1
     ON (in0.v_TZAC = in1.TZAC)

),

Formula_223_0 AS (

  SELECT 
    CAST('Blank' AS STRING) AS v_DebtorCategory,
    CAST(NULL AS STRING) AS v_UnderwritingYear,
    *
  
  FROM Join_217_left_UnionLeftOuter AS in0

),

Join_221_left_UnionLeftOuter AS (

  SELECT 
    in0.* EXCEPT (`Share percent`),
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `TZAC Code`, 
    `CRR`, 
    `Manual TZAC Map`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`)
  
  FROM Formula_223_0 AS in0
  LEFT JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Formula_214_to_Formula_239_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (CAST((SUBSTRING(v_AccountType, 1, 1)) AS STRING) = 'T')
          THEN 'Treaty'
        WHEN (CAST((SUBSTRING(v_AccountType, 1, 1)) AS STRING) = 'O')
          THEN 'FAC - External'
        ELSE 'Not RI Acct'
      END
    ) AS STRING) AS v_RIType,
    CAST((
      CASE
        WHEN CAST((`Trading Partner` IN ('111111', '222222')) AS BOOLEAN)
          THEN 'FAC - IPS'
        ELSE 'Blank'
      END
    ) AS STRING) AS v_RITypeIPS,
    *
  
  FROM Join_221_left_UnionLeftOuter AS in0

),

AlteryxSelect_452 AS (

  SELECT 
    CAST(pPeriod AS STRING) AS Period,
    v_RITypeIPS AS `RI Type IPS`,
    v_RIType AS `RI Type`,
    v_Company AS Company,
    v_RIBranch AS Branch,
    v_Reinsurer AS Reinsurer,
    v_AccountType AS `Account Type`,
    v_AccidentYear AS `Accident Year`,
    v_TZAC AS TZAC,
    v_ProductCode AS `Product Code`,
    v_PremiumClass AS `Premium Class`,
    `Trading Partner` AS `Trading Partner`,
    Rating AS Rating,
    Resident AS Resident,
    v_DebtorCategory AS `Debtor Category`,
    Deemed AS Deemed,
    Authorised AS Authorised,
    v_DebtorsDesc AS `Debtors Description`,
    `Counter Party` AS Counterparty,
    v_Type AS variableType,
    Docs AS Docs,
    `Govn Law` AS `Govn Law`,
    `APRA Identifier` AS `APRA Identifier`,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    `Group Name` AS `Group Name`,
    `Group Domicile` AS `Group Domicile`,
    Country AS Country,
    v_Debtors6Months AS `Debtors > 6 months`,
    v_OSDebtors AS Debtors,
    n_DaysOS AS `Days OS`,
    CAST(NULL AS STRING) AS `RWP Amount`
  
  FROM Formula_214_to_Formula_239_0 AS in0

),

Formula_414_to_Formula_443_0 AS (

  SELECT 
    CAST(NULL AS DOUBLE) AS `ROL Amount`,
    CAST(NULL AS DOUBLE) AS `RUP Amount`,
    CAST(NULL AS DOUBLE) AS `RWP Amount`,
    CAST('Cube_Update_APRA Output_From_APRA Debtors' AS STRING) AS Source,
    * EXCEPT (`rwp amount`)
  
  FROM AlteryxSelect_452 AS in0

),

Formula_297_to_Formula_291_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (CAST((SUBSTRING(v_AccountType, 1, 1)) AS STRING) = 'T')
          THEN 'Treaty'
        WHEN (CAST((SUBSTRING(v_AccountType, 1, 1)) AS STRING) = 'O')
          THEN 'FAC - External'
        ELSE 'Not RI Acct'
      END
    ) AS STRING) AS v_RIType,
    CAST((
      CASE
        WHEN CAST((`Trading Partner` IN ('111111', '222222')) AS BOOLEAN)
          THEN 'FAC - IPS'
        ELSE 'Blank'
      END
    ) AS STRING) AS v_RITypeIPS,
    CAST(NULL AS STRING) AS `Debtors Description`,
    CAST(NULL AS STRING) AS variableType,
    *
  
  FROM Join_304_left_UnionLeftOuter AS in0

),

AlteryxSelect_289 AS (

  SELECT 
    v_Period AS Period,
    v_RITypeIPS AS `RI Type IPS`,
    v_RIType AS `RI Type`,
    v_Company AS Company,
    v_Branch AS Branch,
    v_Reinsurer AS Reinsurer,
    v_AccountType AS `Account Type`,
    v_AccidentYear AS `Accident Year`,
    v_TZAC AS TZAC,
    v_ProductCode AS `Product Code`,
    v_PremiumClass AS `Premium Class`,
    `Trading Partner` AS `Trading Partner`,
    Rating AS Rating,
    Resident AS Resident,
    v_DebtorCategory AS `Debtor Category`,
    Deemed AS Deemed,
    Authorised AS Authorised,
    `Debtors Description` AS `Debtors Description`,
    `Counter Party` AS Counterparty,
    variableType AS variableType,
    Docs AS Docs,
    `Govn Law` AS `Govn Law`,
    `APRA Identifier` AS `APRA Identifier`,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    `Group Name` AS `Group Name`,
    `Group Domicile` AS `Group Domicile`,
    Country AS Country,
    RUP AS `RUP Amount`,
    CAST(NULL AS STRING) AS vROLValue,
    CAST(NULL AS STRING) AS `RWP Amount`
  
  FROM Formula_297_to_Formula_291_0 AS in0

),

Formula_419_to_Formula_444_0 AS (

  SELECT 
    CAST(NULL AS DOUBLE) AS `Debtors > 6 months`,
    CAST(NULL AS DOUBLE) AS Debtors,
    CAST(NULL AS DOUBLE) AS `ROL Amount`,
    CAST(NULL AS STRING) AS `RWP Amount`,
    CAST(NULL AS DOUBLE) AS `Days OS`,
    CAST('Cube_Update_APRA Output_From_Unearned_Income' AS STRING) AS Source,
    * EXCEPT (`rwp amount`)
  
  FROM AlteryxSelect_289 AS in0

),

Union_417_reformat_1 AS (

  SELECT 
    `APRA Identifier` AS `APRA Identifier`,
    `Accident Year` AS `Accident Year`,
    `Account Type` AS `Account Type`,
    Authorised AS Authorised,
    Branch AS Branch,
    Company AS Company,
    Counterparty AS Counterparty,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    Country AS Country,
    `Days OS` AS `Days OS`,
    `Debtor Category` AS `Debtor Category`,
    CAST(Debtors AS DOUBLE) AS Debtors,
    `Debtors > 6 months` AS `Debtors > 6 months`,
    `Debtors Description` AS `Debtors Description`,
    Deemed AS Deemed,
    Docs AS Docs,
    `Govn Law` AS `Govn Law`,
    `Group Domicile` AS `Group Domicile`,
    `Group Name` AS `Group Name`,
    Period AS Period,
    `Premium Class` AS `Premium Class`,
    `Product Code` AS `Product Code`,
    `RI Type` AS `RI Type`,
    `RI Type IPS` AS `RI Type IPS`,
    `ROL Amount` AS `ROL Amount`,
    `RUP Amount` AS `RUP Amount`,
    CAST(`RWP Amount` AS STRING) AS `RWP Amount`,
    Rating AS Rating,
    Reinsurer AS Reinsurer,
    Resident AS Resident,
    Source AS Source,
    TZAC AS TZAC,
    `Trading Partner` AS `Trading Partner`,
    variableType AS variableType,
    CAST(vROLValue AS STRING) AS vROLValue
  
  FROM Formula_419_to_Formula_444_0 AS in0

),

Union_417_reformat_3 AS (

  SELECT 
    `APRA Identifier` AS `APRA Identifier`,
    `Accident Year` AS `Accident Year`,
    `Account Type` AS `Account Type`,
    Authorised AS Authorised,
    Branch AS Branch,
    Company AS Company,
    Counterparty AS Counterparty,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    Country AS Country,
    `Days OS` AS `Days OS`,
    `Debtor Category` AS `Debtor Category`,
    CAST(Debtors AS DOUBLE) AS Debtors,
    `Debtors > 6 months` AS `Debtors > 6 months`,
    `Debtors Description` AS `Debtors Description`,
    Deemed AS Deemed,
    Docs AS Docs,
    `Govn Law` AS `Govn Law`,
    `Group Domicile` AS `Group Domicile`,
    `Group Name` AS `Group Name`,
    Period AS Period,
    `Premium Class` AS `Premium Class`,
    `Product Code` AS `Product Code`,
    `RI Type` AS `RI Type`,
    `RI Type IPS` AS `RI Type IPS`,
    `ROL Amount` AS `ROL Amount`,
    `RUP Amount` AS `RUP Amount`,
    CAST(`RWP Amount` AS STRING) AS `RWP Amount`,
    Rating AS Rating,
    Reinsurer AS Reinsurer,
    Resident AS Resident,
    Source AS Source,
    TZAC AS TZAC,
    `Trading Partner` AS `Trading Partner`,
    variableType AS variableType
  
  FROM Formula_414_to_Formula_443_0 AS in0

),

AlteryxSelect_80 AS (

  SELECT 
    vPeriod AS Period,
    v_RITypeIPS AS `RI Type IPS`,
    v_RIType AS `RI Type`,
    v_Company AS Company,
    v_Branch AS Branch,
    v_Reinsurer AS Reinsurer,
    v_AccountType AS `Account Type`,
    v_AccidentYear AS `Accident Year`,
    v_TZAC AS TZAC,
    v_ProdClass AS `Product Code`,
    v_PremClass AS `Premium Class`,
    `Trading Partner` AS `Trading Partner`,
    Rating AS Rating,
    Resident AS Resident,
    v_DebtorCategory AS `Debtor Category`,
    Deemed AS Deemed,
    Authorised AS Authorised,
    `Debtors Description` AS `Debtors Description`,
    `Counter Party` AS Counterparty,
    variableType AS variableType,
    v_RWP AS `RWP Amount`,
    CAST(NULL AS STRING) AS Docs,
    CAST(NULL AS STRING) AS `Govn Law`,
    CAST(NULL AS STRING) AS `APRA Identifier`,
    CAST(NULL AS STRING) AS `Counterparty Domicile`,
    CAST(NULL AS STRING) AS `Group Name`,
    CAST(NULL AS STRING) AS `Group Domicile`,
    CAST(NULL AS STRING) AS Country
  
  FROM Formula_73_to_Formula_87_0 AS in0

),

Formula_416_to_Formula_441_0 AS (

  SELECT 
    CAST(NULL AS DOUBLE) AS `Debtors > 6 months`,
    CAST(NULL AS DOUBLE) AS Debtors,
    CAST(NULL AS DOUBLE) AS `ROL Amount`,
    CAST(NULL AS DOUBLE) AS `RUP Amount`,
    CAST(NULL AS DOUBLE) AS `Days OS`,
    CAST('Cube_Update_APRA Output_From_APRA RWP' AS STRING) AS Source,
    *
  
  FROM AlteryxSelect_80 AS in0

),

Union_417_reformat_2 AS (

  SELECT 
    `APRA Identifier` AS `APRA Identifier`,
    `Accident Year` AS `Accident Year`,
    `Account Type` AS `Account Type`,
    Authorised AS Authorised,
    Branch AS Branch,
    Company AS Company,
    Counterparty AS Counterparty,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    Country AS Country,
    `Days OS` AS `Days OS`,
    `Debtor Category` AS `Debtor Category`,
    CAST(Debtors AS DOUBLE) AS Debtors,
    `Debtors > 6 months` AS `Debtors > 6 months`,
    `Debtors Description` AS `Debtors Description`,
    Deemed AS Deemed,
    Docs AS Docs,
    `Govn Law` AS `Govn Law`,
    `Group Domicile` AS `Group Domicile`,
    `Group Name` AS `Group Name`,
    Period AS Period,
    `Premium Class` AS `Premium Class`,
    `Product Code` AS `Product Code`,
    `RI Type` AS `RI Type`,
    `RI Type IPS` AS `RI Type IPS`,
    `ROL Amount` AS `ROL Amount`,
    `RUP Amount` AS `RUP Amount`,
    CAST(`RWP Amount` AS STRING) AS `RWP Amount`,
    Rating AS Rating,
    Reinsurer AS Reinsurer,
    Resident AS Resident,
    Source AS Source,
    TZAC AS TZAC,
    `Trading Partner` AS `Trading Partner`,
    variableType AS variableType
  
  FROM Formula_416_to_Formula_441_0 AS in0

),

Union_417 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_417_reformat_2', 'Union_417_reformat_0', 'Union_417_reformat_1', 'Union_417_reformat_3'], 
      [
        '[{"name": "APRA Identifier", "dataType": "String"}, {"name": "Accident Year", "dataType": "String"}, {"name": "Account Type", "dataType": "String"}, {"name": "Authorised", "dataType": "String"}, {"name": "Branch", "dataType": "String"}, {"name": "Company", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "Counterparty Domicile", "dataType": "String"}, {"name": "Country", "dataType": "String"}, {"name": "Days OS", "dataType": "Double"}, {"name": "Debtor Category", "dataType": "String"}, {"name": "Debtors", "dataType": "Double"}, {"name": "Debtors > 6 months", "dataType": "Double"}, {"name": "Debtors Description", "dataType": "String"}, {"name": "Deemed", "dataType": "String"}, {"name": "Docs", "dataType": "String"}, {"name": "Govn Law", "dataType": "String"}, {"name": "Group Domicile", "dataType": "String"}, {"name": "Group Name", "dataType": "String"}, {"name": "Period", "dataType": "String"}, {"name": "Premium Class", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "RI Type", "dataType": "String"}, {"name": "RI Type IPS", "dataType": "String"}, {"name": "ROL Amount", "dataType": "Double"}, {"name": "RUP Amount", "dataType": "Double"}, {"name": "RWP Amount", "dataType": "String"}, {"name": "Rating", "dataType": "String"}, {"name": "Reinsurer", "dataType": "String"}, {"name": "Resident", "dataType": "String"}, {"name": "Source", "dataType": "String"}, {"name": "TZAC", "dataType": "String"}, {"name": "Trading Partner", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
        '[{"name": "APRA Identifier", "dataType": "String"}, {"name": "Accident Year", "dataType": "String"}, {"name": "Account Type", "dataType": "String"}, {"name": "Authorised", "dataType": "String"}, {"name": "Branch", "dataType": "String"}, {"name": "Company", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "Counterparty Domicile", "dataType": "String"}, {"name": "Country", "dataType": "String"}, {"name": "Days OS", "dataType": "Double"}, {"name": "Debtor Category", "dataType": "String"}, {"name": "Debtors", "dataType": "Double"}, {"name": "Debtors > 6 months", "dataType": "Double"}, {"name": "Debtors Description", "dataType": "String"}, {"name": "Deemed", "dataType": "String"}, {"name": "Docs", "dataType": "String"}, {"name": "Govn Law", "dataType": "String"}, {"name": "Group Domicile", "dataType": "String"}, {"name": "Group Name", "dataType": "String"}, {"name": "Period", "dataType": "String"}, {"name": "Premium Class", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "RI Type", "dataType": "String"}, {"name": "RI Type IPS", "dataType": "String"}, {"name": "ROL Amount", "dataType": "Double"}, {"name": "RUP Amount", "dataType": "Double"}, {"name": "RWP Amount", "dataType": "String"}, {"name": "Rating", "dataType": "String"}, {"name": "Reinsurer", "dataType": "String"}, {"name": "Resident", "dataType": "String"}, {"name": "Source", "dataType": "String"}, {"name": "TZAC", "dataType": "String"}, {"name": "Trading Partner", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
        '[{"name": "APRA Identifier", "dataType": "String"}, {"name": "Accident Year", "dataType": "String"}, {"name": "Account Type", "dataType": "String"}, {"name": "Authorised", "dataType": "String"}, {"name": "Branch", "dataType": "String"}, {"name": "Company", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "Counterparty Domicile", "dataType": "String"}, {"name": "Country", "dataType": "String"}, {"name": "Days OS", "dataType": "Double"}, {"name": "Debtor Category", "dataType": "String"}, {"name": "Debtors", "dataType": "Double"}, {"name": "Debtors > 6 months", "dataType": "Double"}, {"name": "Debtors Description", "dataType": "String"}, {"name": "Deemed", "dataType": "String"}, {"name": "Docs", "dataType": "String"}, {"name": "Govn Law", "dataType": "String"}, {"name": "Group Domicile", "dataType": "String"}, {"name": "Group Name", "dataType": "String"}, {"name": "Period", "dataType": "String"}, {"name": "Premium Class", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "RI Type", "dataType": "String"}, {"name": "RI Type IPS", "dataType": "String"}, {"name": "ROL Amount", "dataType": "Double"}, {"name": "RUP Amount", "dataType": "Double"}, {"name": "RWP Amount", "dataType": "String"}, {"name": "Rating", "dataType": "String"}, {"name": "Reinsurer", "dataType": "String"}, {"name": "Resident", "dataType": "String"}, {"name": "Source", "dataType": "String"}, {"name": "TZAC", "dataType": "String"}, {"name": "Trading Partner", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "vROLValue", "dataType": "String"}]', 
        '[{"name": "APRA Identifier", "dataType": "String"}, {"name": "Accident Year", "dataType": "String"}, {"name": "Account Type", "dataType": "String"}, {"name": "Authorised", "dataType": "String"}, {"name": "Branch", "dataType": "String"}, {"name": "Company", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "Counterparty Domicile", "dataType": "String"}, {"name": "Country", "dataType": "String"}, {"name": "Days OS", "dataType": "Double"}, {"name": "Debtor Category", "dataType": "String"}, {"name": "Debtors", "dataType": "Double"}, {"name": "Debtors > 6 months", "dataType": "Double"}, {"name": "Debtors Description", "dataType": "String"}, {"name": "Deemed", "dataType": "String"}, {"name": "Docs", "dataType": "String"}, {"name": "Govn Law", "dataType": "String"}, {"name": "Group Domicile", "dataType": "String"}, {"name": "Group Name", "dataType": "String"}, {"name": "Period", "dataType": "String"}, {"name": "Premium Class", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "RI Type", "dataType": "String"}, {"name": "RI Type IPS", "dataType": "String"}, {"name": "ROL Amount", "dataType": "Double"}, {"name": "RUP Amount", "dataType": "Double"}, {"name": "RWP Amount", "dataType": "String"}, {"name": "Rating", "dataType": "String"}, {"name": "Reinsurer", "dataType": "String"}, {"name": "Resident", "dataType": "String"}, {"name": "Source", "dataType": "String"}, {"name": "TZAC", "dataType": "String"}, {"name": "Trading Partner", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_509_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Reinsurer`, 
    `Account Type`, 
    `Account Name`, 
    `Account Branch`, 
    `TZAC Code`, 
    `CRR`, 
    `Deemed`, 
    `Manual TZAC Map`, 
    `FAC Account Type`, 
    `Broker`, 
    `Address 1`, 
    `Address 2`, 
    `Address 3`, 
    `Postcode`, 
    `GST No`)
  
  FROM Union_417 AS in0
  LEFT JOIN DynamicInput_464 AS in1
     ON (in0.Reinsurer = in1.Reinsurer)

),

Join_510_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`TZAC`, 
    `variableDesc`, 
    `Trading Partner`, 
    `Rating`, 
    `Resident`, 
    `Counter Party`, 
    `Authorised`, 
    `Docs`, 
    `Govn Law`, 
    `APRA Identifier`, 
    `Name of Counterparty`, 
    `Counterparty Domicile`, 
    `Group Name`, 
    `Group Domicile`, 
    `Country`, 
    `variableKey`)
  
  FROM Join_509_left_UnionLeftOuter AS in0
  LEFT JOIN DynamicInput_39 AS in1
     ON (in0.TZAC = in1.TZAC)

),

AlteryxSelect_448 AS (

  SELECT 
    CAST(`RWP Amount` AS DOUBLE) AS `RWP Amount`,
    * EXCEPT (`RWP Amount`)
  
  FROM Join_510_left_UnionLeftOuter AS in0

),

Summarize_413 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((Country IS NULL) OR (CAST(Country AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    SUM(`Debtors > 6 months`) AS `Debtors > 6 months`,
    SUM(Debtors) AS Debtors,
    SUM(`ROL Amount`) AS `ROL Amount`,
    SUM(`RUP Amount`) AS `RUP Amount`,
    SUM(`RWP Amount`) AS `RWP Amount`,
    SUM(`Days OS`) AS `Days OS`,
    `Govn Law` AS `Govn Law`,
    `Debtor Category` AS `Debtor Category`,
    Reinsurer AS Reinsurer,
    Rating AS Rating,
    Authorised AS Authorised,
    Company AS Company,
    `Group Name` AS `Group Name`,
    Deemed AS Deemed,
    Country AS Country,
    `Trading Partner` AS `Trading Partner`,
    `Business Category` AS `Business Category`,
    `Debtors Description` AS `Debtors Description`,
    `Account Type` AS `Account Type`,
    `Occupation Type` AS `Occupation Type`,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    Counterparty AS Counterparty,
    `APRA Grade` AS `APRA Grade`,
    Effective AS Effective,
    `Group Domicile` AS `Group Domicile`,
    Docs AS Docs,
    Period AS Period,
    Valid AS Valid,
    Source AS Source,
    Related AS Related,
    `Accident Year` AS `Accident Year`,
    `Product Code` AS `Product Code`,
    `APRA Identifier` AS `APRA Identifier`,
    TZAC AS TZAC,
    Branch AS Branch,
    `Co-Ins` AS `Co-Ins`,
    `RI Type IPS` AS `RI Type IPS`,
    `Share percent` AS `Share percent`,
    `Premium Class` AS `Premium Class`,
    `RI Type` AS `RI Type`,
    Resident AS Resident,
    variableType AS variableType
  
  FROM AlteryxSelect_448 AS in0
  
  GROUP BY 
    `Govn Law`, 
    `Debtor Category`, 
    Reinsurer, 
    Rating, 
    Authorised, 
    Company, 
    `Group Name`, 
    Deemed, 
    Country, 
    `Trading Partner`, 
    `Business Category`, 
    `Debtors Description`, 
    `Account Type`, 
    `Occupation Type`, 
    `Counterparty Domicile`, 
    Counterparty, 
    `APRA Grade`, 
    Effective, 
    `Group Domicile`, 
    Docs, 
    Period, 
    Valid, 
    Source, 
    Related, 
    `Accident Year`, 
    `Product Code`, 
    `APRA Identifier`, 
    TZAC, 
    Branch, 
    `Co-Ins`, 
    `RI Type IPS`, 
    `Share percent`, 
    `Premium Class`, 
    `RI Type`, 
    Resident, 
    variableType

),

Formula_446_0 AS (

  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS variableTimestamp,
    *
  
  FROM Summarize_413 AS in0

)

SELECT *

FROM Formula_446_0
