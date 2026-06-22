{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH table_79_Output4_macro_op AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_79_Output4_macro_op') }}

),

TextToColumns_204 AS (

  {{
    prophecy_basics.TextToColumns(
      ['table_79_Output4_macro_op'], 
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

TextToColumns_204_dropGem_0 AS (

  SELECT 
    FileName_1_FileName AS FileName1,
    FileName_2_FileName AS FileName2,
    * EXCEPT (`FileName_1_FileName`, `FileName_2_FileName`)
  
  FROM TextToColumns_204 AS in0

),

Cleanse_110 AS (

  {{
    prophecy_basics.DataCleansing(
      ['TextToColumns_204_dropGem_0'], 
      [
        { "name": "FileName1", "dataType": "String" }, 
        { "name": "FileName2", "dataType": "String" }, 
        { "name": "Deal ID", "dataType": "String" }, 
        { "name": "Instrument Description", "dataType": "String" }, 
        { "name": "Inflation Index Ratio", "dataType": "String" }, 
        { "name": "Accrual", "dataType": "String" }, 
        { "name": "Legal Entity", "dataType": "String" }, 
        { "name": "Revision Date", "dataType": "String" }, 
        { "name": "Traded Price Type", "dataType": "String" }, 
        { "name": "Instrument Name", "dataType": "String" }, 
        { "name": "Maturity Date", "dataType": "String" }, 
        { "name": "Traded Clean Price", "dataType": "String" }, 
        { "name": "DV01", "dataType": "String" }, 
        { "name": "Portfolio", "dataType": "String" }, 
        { "name": "Counterparty Name", "dataType": "String" }, 
        { "name": "Trade Date", "dataType": "String" }, 
        { "name": "Counterparty Id", "dataType": "String" }, 
        { "name": "Denominated", "dataType": "String" }, 
        { "name": "Traded Yield", "dataType": "String" }, 
        { "name": "Trade Date EOD Price", "dataType": "String" }, 
        { "name": "Notional", "dataType": "String" }, 
        { "name": "FileName", "dataType": "String" }, 
        { "name": "ISIN", "dataType": "String" }, 
        { "name": "Traded Price", "dataType": "String" }, 
        { "name": "Debt Type", "dataType": "String" }
      ], 
      'keepOriginal', 
      [
        'Revision Date', 
        'Deal ID', 
        'Trade Date', 
        'ISIN', 
        'Instrument Description', 
        'Instrument Name', 
        'Denominated', 
        'Maturity Date', 
        'Traded Price', 
        'Traded Price Type', 
        'Notional', 
        'Accrual', 
        'Traded Yield', 
        'Traded Clean Price', 
        'Trade Date EOD Price', 
        'Inflation Index Ratio', 
        'Portfolio', 
        'Legal Entity', 
        'DV01', 
        'Counterparty Id', 
        'Counterparty Name', 
        'Debt Type', 
        'FileName'
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

Formula_111_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Notional, '^[\\)]+|[\\)]+$', '')) AS string) AS Notional,
    * EXCEPT (`notional`)
  
  FROM Cleanse_110 AS in0

),

Formula_111_1 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Notional, '(', '-')) AS string) AS Notional,
    * EXCEPT (`notional`)
  
  FROM Formula_111_0 AS in0

),

Formula_111_2 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Notional, ',', '')) AS string) AS Notional,
    CAST(FileName1 AS string) AS Region,
    * EXCEPT (`notional`)
  
  FROM Formula_111_1 AS in0

),

Filter_223 AS (

  SELECT * 
  
  FROM Formula_111_2 AS in0
  
  WHERE contains(Region, 'Asia')

),

AlteryxSelect_80 AS (

  SELECT 
    ISIN AS ISIN,
    CAST(Notional AS DOUBLE) AS Notional,
    FileName AS FileName,
    Region AS Region,
    `Revision Date` AS `Revision Date`,
    `Trade Date` AS `Trade Date`,
    `Instrument Description` AS `Instrument Description`,
    Denominated AS Denominated,
    `Counterparty Name` AS `Counterparty Name`
  
  FROM Filter_223 AS in0

),

Union_222_reformat_0 AS (

  SELECT 
    `Counterparty Name` AS `Counterparty Name`,
    Denominated AS Denominated,
    FileName AS FileName,
    ISIN AS ISIN,
    `Instrument Description` AS `Instrument Description`,
    CAST(Notional AS string) AS Notional,
    Region AS Region,
    `Revision Date` AS `Revision Date`,
    `Trade Date` AS `Trade Date`
  
  FROM AlteryxSelect_80 AS in0

),

Filter_223_reject AS (

  SELECT * 
  
  FROM Formula_111_2 AS in0
  
  WHERE (NOT (contains(Region, 'Asia')) OR isnull(contains(Region, 'Asia')))

),

Formula_224_0 AS (

  SELECT 
    CAST(`Trade Date` AS string) AS `Revision Date`,
    * EXCEPT (`revision date`)
  
  FROM Filter_223_reject AS in0

),

Union_222_reformat_1 AS (

  SELECT 
    CAST(Accrual AS string) AS Accrual,
    CAST(`Counterparty Id` AS string) AS `Counterparty Id`,
    `Counterparty Name` AS `Counterparty Name`,
    CAST(DV01 AS string) AS DV01,
    CAST(`Deal ID` AS string) AS `Deal ID`,
    CAST(`Debt Type` AS string) AS `Debt Type`,
    Denominated AS Denominated,
    FileName AS FileName,
    CAST(FileName1 AS string) AS FileName1,
    CAST(FileName2 AS string) AS FileName2,
    ISIN AS ISIN,
    CAST(`Inflation Index Ratio` AS string) AS `Inflation Index Ratio`,
    `Instrument Description` AS `Instrument Description`,
    CAST(`Instrument Name` AS string) AS `Instrument Name`,
    CAST(`Legal Entity` AS string) AS `Legal Entity`,
    CAST(`Maturity Date` AS string) AS `Maturity Date`,
    CAST(Notional AS string) AS Notional,
    CAST(Portfolio AS string) AS Portfolio,
    Region AS Region,
    `Revision Date` AS `Revision Date`,
    `Trade Date` AS `Trade Date`,
    CAST(`Trade Date EOD Price` AS string) AS `Trade Date EOD Price`,
    CAST(`Traded Clean Price` AS string) AS `Traded Clean Price`,
    CAST(`Traded Price` AS string) AS `Traded Price`,
    CAST(`Traded Price Type` AS string) AS `Traded Price Type`,
    CAST(`Traded Yield` AS string) AS `Traded Yield`
  
  FROM Formula_224_0 AS in0

),

Union_222 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_222_reformat_0', 'Union_222_reformat_1'], 
      [
        '[{"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}]', 
        '[{"name": "Accrual", "dataType": "String"}, {"name": "Counterparty Id", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "DV01", "dataType": "String"}, {"name": "Deal ID", "dataType": "String"}, {"name": "Debt Type", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Inflation Index Ratio", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Instrument Name", "dataType": "String"}, {"name": "Legal Entity", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Portfolio", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Trade Date EOD Price", "dataType": "String"}, {"name": "Traded Clean Price", "dataType": "String"}, {"name": "Traded Price", "dataType": "String"}, {"name": "Traded Price Type", "dataType": "String"}, {"name": "Traded Yield", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

DateTime_81_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`Revision Date`, 'MM/dd/yyyy')), 'yyyy-MM-dd')) AS variableDate,
    *
  
  FROM Union_222 AS in0

),

DateTime_126_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`Trade Date`, 'MM/dd/yyyy')), 'yyyy-MM-dd')) AS Date_fx,
    *
  
  FROM DateTime_81_0 AS in0

),

Formula_82_0 AS (

  SELECT 
    CAST(month(variableDate) AS STRING) AS Month,
    *
  
  FROM DateTime_126_0 AS in0

)

SELECT *

FROM Formula_82_0
