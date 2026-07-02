{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_31 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__AlteryxSelect_31')}}

),

Configuration_t_35 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume', 'Configuration_t_35') }}

),

Filter_34 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__Filter_34')}}

),

Summarize_14 AS (

  SELECT 
    DISTINCT Counterparty AS Counterparty,
    `Y/N?` AS `Y/N?`
  
  FROM Configuration_t_35 AS in0

),

Join_33_left AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__Join_33_left')}}

),

Formula_43_0 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__Formula_43_0')}}

),

Filter_34_reject AS (

  SELECT * 
  
  FROM Formula_43_0 AS in0
  
  WHERE (
          (NOT(((ISIN IS NULL) OR (ISIN IS NULL)) OR ((LENGTH(ISIN)) = 0)))
          OR ((((ISIN IS NULL) OR (ISIN IS NULL)) OR ((LENGTH(ISIN)) = 0)) IS NULL)
        )

),

Formula_15_0 AS (

  SELECT 
    CAST('Y' AS string) AS `ISIN in Scope?`,
    *
  
  FROM Filter_34_reject AS in0

),

Formula_17_0 AS (

  SELECT 
    CAST('N' AS string) AS `ISIN in Scope?`,
    *
  
  FROM Join_33_left AS in0

),

Join_33_inner AS (

  SELECT 
    in0.`Trade Date` AS `Trade Date`,
    in0.Denominated AS Denominated,
    in1.ISIN AS ISIN,
    in0.Notional AS Notional,
    in0.`Revision Date` AS `Revision Date`,
    in0.`Counterparty Name` AS `Counterparty Name`,
    in0.Date_fx AS Date_fx,
    in0.variableDate AS variableDate,
    in0.Region AS Region,
    in0.`Instrument Description` AS `Instrument Description`,
    in0.Month AS Month
  
  FROM Filter_34 AS in0
  INNER JOIN AlteryxSelect_31 AS in1
     ON (in0.`Instrument Description` = in1.`Instrument Description`)

),

Formula_16_0 AS (

  SELECT 
    CAST('Y' AS string) AS `ISIN in Scope?`,
    *
  
  FROM Join_33_inner AS in0

),

Union_32_split AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_16_0', 'Formula_17_0'], 
      [
        '[{"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Month", "dataType": "String"}]', 
        '[{"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrued Interest", "dataType": "String"}, {"name": "Bond Description", "dataType": "String"}, {"name": "Book", "dataType": "String"}, {"name": "Buy/Sell", "dataType": "String"}, {"name": "Clean Price", "dataType": "String"}, {"name": "Consideration", "dataType": "String"}, {"name": "Consideration (USD)", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "Counterparty LEI", "dataType": "String"}, {"name": "Coupon (%)", "dataType": "String"}, {"name": "Currency", "dataType": "String"}, {"name": "Desk", "dataType": "String"}, {"name": "Dirty Price", "dataType": "String"}, {"name": "FX Rate (vs USD)", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Notional (USD)", "dataType": "String"}, {"name": "Product Type", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Settlement Date", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Trade ID", "dataType": "String"}, {"name": "Trader", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_32 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_15_0', 'Union_32_split'], 
      [
        '[{"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrued Interest", "dataType": "String"}, {"name": "Bond Description", "dataType": "String"}, {"name": "Book", "dataType": "String"}, {"name": "Buy/Sell", "dataType": "String"}, {"name": "Clean Price", "dataType": "String"}, {"name": "Consideration", "dataType": "String"}, {"name": "Consideration (USD)", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "Counterparty LEI", "dataType": "String"}, {"name": "Coupon (%)", "dataType": "String"}, {"name": "Currency", "dataType": "String"}, {"name": "Desk", "dataType": "String"}, {"name": "Dirty Price", "dataType": "String"}, {"name": "FX Rate (vs USD)", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Notional (USD)", "dataType": "String"}, {"name": "Product Type", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Settlement Date", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Trade ID", "dataType": "String"}, {"name": "Trader", "dataType": "String"}]', 
        '[{"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "Accrued Interest", "dataType": "String"}, {"name": "Bond Description", "dataType": "String"}, {"name": "Book", "dataType": "String"}, {"name": "Buy/Sell", "dataType": "String"}, {"name": "Clean Price", "dataType": "String"}, {"name": "Consideration", "dataType": "String"}, {"name": "Consideration (USD)", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "Counterparty LEI", "dataType": "String"}, {"name": "Coupon (%)", "dataType": "String"}, {"name": "Currency", "dataType": "String"}, {"name": "Desk", "dataType": "String"}, {"name": "Dirty Price", "dataType": "String"}, {"name": "FX Rate (vs USD)", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Notional (USD)", "dataType": "String"}, {"name": "Product Type", "dataType": "String"}, {"name": "Settlement Date", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Trade ID", "dataType": "String"}, {"name": "Trader", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_30 AS (

  SELECT * 
  
  FROM Union_32 AS in0
  
  WHERE (
          ((`Counterparty Name` IS NULL) OR (`Counterparty Name` IS NULL))
          OR ((LENGTH(`Counterparty Name`)) = 0)
        )

),

Formula_28_0 AS (

  SELECT 
    CAST('N' AS string) AS `Y/N?`,
    *
  
  FROM Filter_30 AS in0

),

Filter_30_reject AS (

  SELECT * 
  
  FROM Union_32 AS in0
  
  WHERE (
          (
            NOT(
              ((`Counterparty Name` IS NULL) OR (`Counterparty Name` IS NULL))
              OR ((LENGTH(`Counterparty Name`)) = 0))
          )
          OR (
               (
                 ((`Counterparty Name` IS NULL) OR (`Counterparty Name` IS NULL))
                 OR ((LENGTH(`Counterparty Name`)) = 0)
               ) IS NULL
             )
        )

),

Join_29_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Counterparty Name` = in1.Counterparty)
          THEN NULL
        ELSE 'N'
      END
    ) AS `Y/N?`,
    in0.* EXCEPT (`Counterparty`),
    in1.* EXCEPT (`Y/N?`)
  
  FROM Filter_30_reject AS in0
  LEFT JOIN Summarize_14 AS in1
     ON (in0.`Counterparty Name` = in1.Counterparty)

),

Union_26 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_28_0', 'Join_29_inner_UnionLeftOuter'], 
      [
        '[{"name": "Y/N?", "dataType": "String"}, {"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrued Interest", "dataType": "String"}, {"name": "Bond Description", "dataType": "String"}, {"name": "Book", "dataType": "String"}, {"name": "Buy/Sell", "dataType": "String"}, {"name": "Clean Price", "dataType": "String"}, {"name": "Consideration", "dataType": "String"}, {"name": "Consideration (USD)", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "Counterparty LEI", "dataType": "String"}, {"name": "Coupon (%)", "dataType": "String"}, {"name": "Currency", "dataType": "String"}, {"name": "Desk", "dataType": "String"}, {"name": "Dirty Price", "dataType": "String"}, {"name": "FX Rate (vs USD)", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Notional (USD)", "dataType": "String"}, {"name": "Product Type", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Settlement Date", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Trade ID", "dataType": "String"}, {"name": "Trader", "dataType": "String"}]', 
        '[{"name": "Y/N?", "dataType": "String"}, {"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrued Interest", "dataType": "String"}, {"name": "Bond Description", "dataType": "String"}, {"name": "Book", "dataType": "String"}, {"name": "Buy/Sell", "dataType": "String"}, {"name": "Clean Price", "dataType": "String"}, {"name": "Consideration", "dataType": "String"}, {"name": "Consideration (USD)", "dataType": "String"}, {"name": "Counterparty LEI", "dataType": "String"}, {"name": "Coupon (%)", "dataType": "String"}, {"name": "Currency", "dataType": "String"}, {"name": "Desk", "dataType": "String"}, {"name": "Dirty Price", "dataType": "String"}, {"name": "FX Rate (vs USD)", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Notional (USD)", "dataType": "String"}, {"name": "Product Type", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Settlement Date", "dataType": "String"}, {"name": "Status", "dataType": "String"}, {"name": "Trade ID", "dataType": "String"}, {"name": "Trader", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_26
