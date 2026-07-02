{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH table_57_Output4_macro_op AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_57_Output4_macro_op') }}

),

TextToColumns_48 AS (

  {{
    prophecy_basics.TextToColumns(
      ['table_57_Output4_macro_op'], 
      'FileName', 
      "\[_\]", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'FileName', 
      'FileName', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_48_dropGem_0 AS (

  SELECT 
    FileName_1_FileName AS FileName1,
    FileName_2_FileName AS FileName2,
    * EXCEPT (`FileName_1_FileName`, `FileName_2_FileName`)
  
  FROM TextToColumns_48 AS in0

),

Cleanse_23 AS (

  {{
    prophecy_basics.DataCleansing(
      ['TextToColumns_48_dropGem_0'], 
      [
        { "name": "FileName1", "dataType": "String" }, 
        { "name": "FileName2", "dataType": "String" }, 
        { "name": "Instrument Description", "dataType": "String" }, 
        { "name": "Counterparty LEI", "dataType": "String" }, 
        { "name": "Clean Price", "dataType": "String" }, 
        { "name": "Trader", "dataType": "String" }, 
        { "name": "Desk", "dataType": "String" }, 
        { "name": "Coupon (%)", "dataType": "String" }, 
        { "name": "Maturity Date", "dataType": "String" }, 
        { "name": "Product Type", "dataType": "String" }, 
        { "name": "Region", "dataType": "String" }, 
        { "name": "Consideration", "dataType": "String" }, 
        { "name": "Counterparty", "dataType": "String" }, 
        { "name": "Counterparty Name", "dataType": "String" }, 
        { "name": "Settlement Date", "dataType": "String" }, 
        { "name": "Trade Date", "dataType": "String" }, 
        { "name": "Denominated", "dataType": "String" }, 
        { "name": "Book", "dataType": "String" }, 
        { "name": "Notional", "dataType": "String" }, 
        { "name": "Currency", "dataType": "String" }, 
        { "name": "FileName", "dataType": "String" }, 
        { "name": "Consideration (USD)", "dataType": "String" }, 
        { "name": "Accrued Interest", "dataType": "String" }, 
        { "name": "FX Rate (vs USD)", "dataType": "String" }, 
        { "name": "ISIN", "dataType": "String" }, 
        { "name": "Dirty Price", "dataType": "String" }, 
        { "name": "Notional (USD)", "dataType": "String" }, 
        { "name": "Trade ID", "dataType": "String" }, 
        { "name": "Buy/Sell", "dataType": "String" }, 
        { "name": "Bond Description", "dataType": "String" }, 
        { "name": "Status", "dataType": "String" }
      ], 
      'keepOriginal', 
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

Formula_22_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Notional, '^[\\)]+|[\\)]+$', '')) AS string) AS Notional,
    * EXCEPT (`notional`)
  
  FROM Cleanse_23 AS in0

),

Formula_22_1 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Notional, '(', '-')) AS string) AS Notional,
    * EXCEPT (`notional`)
  
  FROM Formula_22_0 AS in0

),

Formula_22_2 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Notional, ',', '')) AS string) AS Notional,
    CAST(FileName1 AS string) AS Region,
    * EXCEPT (`region`, `notional`)
  
  FROM Formula_22_1 AS in0

),

Filter_52_reject AS (

  SELECT * 
  
  FROM Formula_22_2 AS in0
  
  WHERE (NOT (contains(Region, 'Asia')) OR isnull(contains(Region, 'Asia')))

),

Formula_53_0 AS (

  SELECT 
    CAST(`Trade Date` AS string) AS `Revision Date`,
    *
  
  FROM Filter_52_reject AS in0

),

Union_51_reformat_1 AS (

  SELECT 
    CAST(`Accrued Interest` AS string) AS `Accrued Interest`,
    CAST(`Bond Description` AS string) AS `Bond Description`,
    CAST(Book AS string) AS Book,
    CAST(`Buy/Sell` AS string) AS `Buy/Sell`,
    CAST(`Clean Price` AS string) AS `Clean Price`,
    CAST(Consideration AS string) AS Consideration,
    CAST(`Consideration (USD)` AS string) AS `Consideration (USD)`,
    CAST(Counterparty AS string) AS Counterparty,
    CAST(`Counterparty LEI` AS string) AS `Counterparty LEI`,
    `Counterparty Name` AS `Counterparty Name`,
    CAST(`Coupon (%)` AS string) AS `Coupon (%)`,
    CAST(Currency AS string) AS Currency,
    Denominated AS Denominated,
    CAST(Desk AS string) AS Desk,
    CAST(`Dirty Price` AS string) AS `Dirty Price`,
    CAST(`FX Rate (vs USD)` AS string) AS `FX Rate (vs USD)`,
    FileName AS FileName,
    CAST(FileName1 AS string) AS FileName1,
    CAST(FileName2 AS string) AS FileName2,
    ISIN AS ISIN,
    `Instrument Description` AS `Instrument Description`,
    CAST(`Maturity Date` AS string) AS `Maturity Date`,
    CAST(Notional AS string) AS Notional,
    CAST(`Notional (USD)` AS string) AS `Notional (USD)`,
    CAST(`Product Type` AS string) AS `Product Type`,
    Region AS Region,
    CAST(`Revision Date` AS string) AS `Revision Date`,
    CAST(`Settlement Date` AS string) AS `Settlement Date`,
    CAST(Status AS string) AS Status,
    `Trade Date` AS `Trade Date`,
    CAST(`Trade ID` AS string) AS `Trade ID`,
    CAST(Trader AS string) AS Trader
  
  FROM Formula_53_0 AS in0

),

Filter_52 AS (

  SELECT * 
  
  FROM Formula_22_2 AS in0
  
  WHERE contains(Region, 'Asia')

),

AlteryxSelect_45 AS (

  SELECT 
    `Trade Date` AS `Trade Date`,
    ISIN AS ISIN,
    `Instrument Description` AS `Instrument Description`,
    Region AS Region,
    CAST(Notional AS DOUBLE) AS Notional,
    Denominated AS Denominated,
    `Counterparty Name` AS `Counterparty Name`,
    FileName AS FileName
  
  FROM Filter_52 AS in0

),

Union_51_reformat_0 AS (

  SELECT 
    `Counterparty Name` AS `Counterparty Name`,
    Denominated AS Denominated,
    FileName AS FileName,
    ISIN AS ISIN,
    `Instrument Description` AS `Instrument Description`,
    CAST(Notional AS string) AS Notional,
    Region AS Region,
    `Trade Date` AS `Trade Date`
  
  FROM AlteryxSelect_45 AS in0

),

Union_51 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_51_reformat_0', 'Union_51_reformat_1'], 
      [
        '[{"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}]', 
        '[{"name": "Accrued Interest", "dataType": "String"}, {"name": "Bond Description", "dataType": "String"}, {"name": "Book", "dataType": "String"}, {"name": "Buy/Sell", "dataType": "String"}, {"name": "Clean Price", "dataType": "String"}, {"name": "Consideration", "dataType": "String"}, {"name": "Consideration (USD)", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "Counterparty LEI", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Coupon (%)", "dataType": "String"}, {"name": "Currency", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "Desk", "dataType": "String"}, {"name": "Dirty Price", "dataType": "String"}, {"name": "FX Rate (vs USD)", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Notional (USD)", "dataType": "String"}, {"name": "Product Type", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Settlement Date", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Trade ID", "dataType": "String"}, {"name": "Trader", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

DateTime_44_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`Revision Date`, 'MM/dd/yyyy')), 'yyyy-MM-dd')) AS variableDate,
    *
  
  FROM Union_51 AS in0

),

DateTime_12_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`Trade Date`, 'MM/dd/yyyy')), 'yyyy-MM-dd')) AS Date_fx,
    *
  
  FROM DateTime_44_0 AS in0

),

Formula_43_0 AS (

  SELECT 
    CAST(month(variableDate) AS STRING) AS Month,
    *
  
  FROM DateTime_12_0 AS in0

)

SELECT *

FROM Formula_43_0
