{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Filter_93 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1__Filter_93')}}

),

AlteryxSelect_96 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1__AlteryxSelect_96')}}

),

Join_94_inner AS (

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
  
  FROM Filter_93 AS in0
  INNER JOIN AlteryxSelect_96 AS in1
     ON (in0.`Instrument Description` = in1.`Instrument Description`)

),

Formula_119_0 AS (

  SELECT 
    CAST('Y' AS string) AS `ISIN in Scope?`,
    *
  
  FROM Join_94_inner AS in0

),

Join_94_left AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1__Join_94_left')}}

),

Formula_118_0 AS (

  SELECT 
    CAST('N' AS string) AS `ISIN in Scope?`,
    *
  
  FROM Join_94_left AS in0

),

Union_95_split AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_119_0', 'Formula_118_0'], 
      [
        '[{"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Month", "dataType": "String"}]', 
        '[{"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrual", "dataType": "String"}, {"name": "Counterparty Id", "dataType": "String"}, {"name": "DV01", "dataType": "String"}, {"name": "Deal ID", "dataType": "String"}, {"name": "Debt Type", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Inflation Index Ratio", "dataType": "String"}, {"name": "Instrument Name", "dataType": "String"}, {"name": "Legal Entity", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Portfolio", "dataType": "String"}, {"name": "Trade Date EOD Price", "dataType": "String"}, {"name": "Traded Clean Price", "dataType": "String"}, {"name": "Traded Price", "dataType": "String"}, {"name": "Traded Price Type", "dataType": "String"}, {"name": "Traded Yield", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_82_0 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1__Formula_82_0')}}

),

Filter_93_reject AS (

  SELECT * 
  
  FROM Formula_82_0 AS in0
  
  WHERE (
          (NOT(((ISIN IS NULL) OR (ISIN IS NULL)) OR ((LENGTH(ISIN)) = 0)))
          OR ((((ISIN IS NULL) OR (ISIN IS NULL)) OR ((LENGTH(ISIN)) = 0)) IS NULL)
        )

),

Formula_120_0 AS (

  SELECT 
    CAST('Y' AS string) AS `ISIN in Scope?`,
    *
  
  FROM Filter_93_reject AS in0

),

Union_95 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_120_0', 'Union_95_split'], 
      [
        '[{"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrual", "dataType": "String"}, {"name": "Counterparty Id", "dataType": "String"}, {"name": "DV01", "dataType": "String"}, {"name": "Deal ID", "dataType": "String"}, {"name": "Debt Type", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Inflation Index Ratio", "dataType": "String"}, {"name": "Instrument Name", "dataType": "String"}, {"name": "Legal Entity", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Portfolio", "dataType": "String"}, {"name": "Trade Date EOD Price", "dataType": "String"}, {"name": "Traded Clean Price", "dataType": "String"}, {"name": "Traded Price", "dataType": "String"}, {"name": "Traded Price Type", "dataType": "String"}, {"name": "Traded Yield", "dataType": "String"}]', 
        '[{"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "Accrual", "dataType": "String"}, {"name": "Counterparty Id", "dataType": "String"}, {"name": "DV01", "dataType": "String"}, {"name": "Deal ID", "dataType": "String"}, {"name": "Debt Type", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Inflation Index Ratio", "dataType": "String"}, {"name": "Instrument Name", "dataType": "String"}, {"name": "Legal Entity", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Portfolio", "dataType": "String"}, {"name": "Trade Date EOD Price", "dataType": "String"}, {"name": "Traded Clean Price", "dataType": "String"}, {"name": "Traded Price", "dataType": "String"}, {"name": "Traded Price Type", "dataType": "String"}, {"name": "Traded Yield", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_99 AS (

  SELECT * 
  
  FROM Union_95 AS in0
  
  WHERE (
          ((`Counterparty Name` IS NULL) OR (`Counterparty Name` IS NULL))
          OR ((LENGTH(`Counterparty Name`)) = 0)
        )

),

Formula_101_0 AS (

  SELECT 
    CAST('N' AS string) AS `Y/N?`,
    *
  
  FROM Filter_99 AS in0

),

Configuration_t_90 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1', 'Configuration_t_90') }}

),

Summarize_123 AS (

  SELECT 
    DISTINCT Counterparty AS Counterparty,
    `Y/N?` AS `Y/N?`
  
  FROM Configuration_t_90 AS in0

),

Filter_99_reject AS (

  SELECT * 
  
  FROM Union_95 AS in0
  
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

Join_100_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Counterparty Name` = in1.Counterparty)
          THEN NULL
        ELSE 'N'
      END
    ) AS `Y/N?`,
    in0.*,
    in1.* EXCEPT (`Y/N?`)
  
  FROM Filter_99_reject AS in0
  LEFT JOIN Summarize_123 AS in1
     ON (in0.`Counterparty Name` = in1.Counterparty)

),

Union_103 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_101_0', 'Join_100_inner_UnionLeftOuter'], 
      [
        '[{"name": "Y/N?", "dataType": "String"}, {"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrual", "dataType": "String"}, {"name": "Counterparty Id", "dataType": "String"}, {"name": "DV01", "dataType": "String"}, {"name": "Deal ID", "dataType": "String"}, {"name": "Debt Type", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Inflation Index Ratio", "dataType": "String"}, {"name": "Instrument Name", "dataType": "String"}, {"name": "Legal Entity", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Portfolio", "dataType": "String"}, {"name": "Trade Date EOD Price", "dataType": "String"}, {"name": "Traded Clean Price", "dataType": "String"}, {"name": "Traded Price", "dataType": "String"}, {"name": "Traded Price Type", "dataType": "String"}, {"name": "Traded Yield", "dataType": "String"}]', 
        '[{"name": "Y/N?", "dataType": "String"}, {"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrual", "dataType": "String"}, {"name": "Counterparty Id", "dataType": "String"}, {"name": "DV01", "dataType": "String"}, {"name": "Deal ID", "dataType": "String"}, {"name": "Debt Type", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Inflation Index Ratio", "dataType": "String"}, {"name": "Instrument Name", "dataType": "String"}, {"name": "Legal Entity", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Portfolio", "dataType": "String"}, {"name": "Trade Date EOD Price", "dataType": "String"}, {"name": "Traded Clean Price", "dataType": "String"}, {"name": "Traded Price", "dataType": "String"}, {"name": "Traded Price Type", "dataType": "String"}, {"name": "Traded Yield", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_103
