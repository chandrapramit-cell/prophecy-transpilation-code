{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_83 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew', 
      'Configuration_t_83'
    )
  }}

),

AlteryxSelect_84 AS (

  SELECT 
    CAST(Month AS string) AS Month,
    Weights AS Weights
  
  FROM Configuration_t_83 AS in0

),

Union_103 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Union_103')}}

),

Configuration_t_137 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew', 
      'Configuration_t_137'
    )
  }}

),

Summarize_138 AS (

  SELECT 
    DISTINCT ISIN AS ISIN,
    `Product Type` AS `Product Type`,
    Region AS Region
  
  FROM Configuration_t_137 AS in0

),

Sample_86 AS (

  {{
    prophecy_basics.Sample(
      ['Configuration_t_83'], 
      '[{"name": "Month", "dataType": "Date"}, {"name": "Weights", "dataType": "Double"}, {"name": "Number of working Days", "dataType": "Double"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

AlteryxSelect_85 AS (

  SELECT `Number of working Days` AS `Number of working Days`
  
  FROM Sample_86 AS in0

),

Formula_87_0 AS (

  SELECT 
    CAST(month(MONTH) AS STRING) AS Month,
    * EXCEPT (`month`)
  
  FROM AlteryxSelect_84 AS in0

),

Filter_206_reject AS (

  SELECT * 
  
  FROM Union_103 AS in0
  
  WHERE (
          (
            NOT(
              Region = 'ASIA')
          ) OR ((Region = 'ASIA') IS NULL)
        )

),

Summarize_124 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Summarize_124')}}

),

Filter_206 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Filter_206')}}

),

Join_107_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`variableDate`, `Currency`)
  
  FROM Filter_206 AS in0
  INNER JOIN Summarize_124 AS in1
     ON ((CAST(in0.Date_fx AS DATE) = in1.variableDate) AND (in0.Denominated = in1.Currency))

),

Union_205 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_107_inner', 'Filter_206_reject'], 
      [
        '[{"name": "Y/N?", "dataType": "String"}, {"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrual", "dataType": "String"}, {"name": "Counterparty Id", "dataType": "String"}, {"name": "DV01", "dataType": "String"}, {"name": "Deal ID", "dataType": "String"}, {"name": "Debt Type", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Inflation Index Ratio", "dataType": "String"}, {"name": "Instrument Name", "dataType": "String"}, {"name": "Legal Entity", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Portfolio", "dataType": "String"}, {"name": "Trade Date EOD Price", "dataType": "String"}, {"name": "Traded Clean Price", "dataType": "String"}, {"name": "Traded Price", "dataType": "String"}, {"name": "Traded Price Type", "dataType": "String"}, {"name": "Traded Yield", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}, {"name": "FX", "dataType": "Double"}]', 
        '[{"name": "Y/N?", "dataType": "String"}, {"name": "ISIN in Scope?", "dataType": "String"}, {"name": "Month", "dataType": "String"}, {"name": "Date_fx", "dataType": "String"}, {"name": "variableDate", "dataType": "String"}, {"name": "Counterparty Name", "dataType": "String"}, {"name": "Denominated", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ISIN", "dataType": "String"}, {"name": "Instrument Description", "dataType": "String"}, {"name": "Notional", "dataType": "String"}, {"name": "Region", "dataType": "String"}, {"name": "Revision Date", "dataType": "String"}, {"name": "Trade Date", "dataType": "String"}, {"name": "Accrual", "dataType": "String"}, {"name": "Counterparty Id", "dataType": "String"}, {"name": "DV01", "dataType": "String"}, {"name": "Deal ID", "dataType": "String"}, {"name": "Debt Type", "dataType": "String"}, {"name": "FileName1", "dataType": "String"}, {"name": "FileName2", "dataType": "String"}, {"name": "Inflation Index Ratio", "dataType": "String"}, {"name": "Instrument Name", "dataType": "String"}, {"name": "Legal Entity", "dataType": "String"}, {"name": "Maturity Date", "dataType": "String"}, {"name": "Portfolio", "dataType": "String"}, {"name": "Trade Date EOD Price", "dataType": "String"}, {"name": "Traded Clean Price", "dataType": "String"}, {"name": "Traded Price", "dataType": "String"}, {"name": "Traded Price Type", "dataType": "String"}, {"name": "Traded Yield", "dataType": "String"}, {"name": "Counterparty", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_109_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Region = 'ASIA')
          THEN (ABS(CAST(Notional AS DECIMAL (19, 9))) / FX)
        ELSE ABS(CAST(Notional AS DECIMAL (19, 9)))
      END
    ) AS DOUBLE) AS `Abs Notional (USD)`,
    *
  
  FROM Union_205 AS in0

),

Join_112_inner AS (

  SELECT 
    in0.`ISIN in Scope?` AS `ISIN in Scope?`,
    in0.`Trade Date` AS `Trade Date`,
    in0.Denominated AS Denominated,
    in0.`Y/N?` AS `Y/N?`,
    in0.ISIN AS ISIN,
    in1.Weights AS Weights,
    in0.Notional AS Notional,
    in0.`Revision Date` AS `Revision Date`,
    in0.FX AS FX,
    in0.`Counterparty Name` AS `Counterparty Name`,
    in0.`Abs Notional (USD)` AS `Abs Notional (USD)`,
    in0.variableDate AS variableDate,
    in0.Region AS Region,
    in0.`Instrument Description` AS `Instrument Description`,
    in0.Month AS Month
  
  FROM Formula_109_0 AS in0
  INNER JOIN Formula_87_0 AS in1
     ON (in0.Month = in1.Month)

),

AppendFields_113 AS (

  SELECT 
    in1.`ISIN in Scope?` AS `ISIN in Scope?`,
    in1.`Trade Date` AS `Trade Date`,
    in1.Denominated AS Denominated,
    in1.`Y/N?` AS `Y/N?`,
    in1.ISIN AS ISIN,
    in1.Weights AS Weights,
    in0.`Number of working Days` AS `Number of working Days`,
    in1.Notional AS Notional,
    in1.`Revision Date` AS `Revision Date`,
    in1.FX AS FX,
    in1.`Counterparty Name` AS `Counterparty Name`,
    in1.`Abs Notional (USD)` AS `Abs Notional (USD)`,
    in1.variableDate AS variableDate,
    in1.Region AS Region,
    in1.`Instrument Description` AS `Instrument Description`,
    in1.Month AS Month
  
  FROM AlteryxSelect_85 AS in0
  INNER JOIN Join_112_inner AS in1
     ON TRUE

),

Formula_114_0 AS (

  SELECT 
    CAST(((`Abs Notional (USD)` / `Number of working Days`) * Weights) AS DOUBLE) AS `Weighted-Average`,
    *
  
  FROM AppendFields_113 AS in0

),

Filter_115_to_Filter_127 AS (

  SELECT * 
  
  FROM Formula_114_0 AS in0
  
  WHERE ((`Y/N?` = 'N') AND (`ISIN in Scope?` = 'Y'))

),

Summarize_134 AS (

  SELECT 
    SUM(`Weighted-Average`) AS `Weighted-Average`,
    `Instrument Description` AS `Instrument Description`,
    Month AS Month,
    Region AS filename,
    Denominated AS `Currency Denomination`,
    ISIN AS ISIN
  
  FROM Filter_115_to_Filter_127 AS in0
  
  GROUP BY 
    `Instrument Description`, Month, Region, Denominated, ISIN

),

Join_136_inner AS (

  SELECT 
    in0.ISIN AS ISIN,
    in0.`Weighted-Average` AS `Weighted-Average`,
    in0.`Currency Denomination` AS `Currency Denomination`,
    in0.filename AS filename,
    in1.Region AS Region,
    in1.`Product Type` AS `Product Type`,
    in0.`Instrument Description` AS `Instrument Description`,
    in0.Month AS Month
  
  FROM Summarize_134 AS in0
  INNER JOIN Summarize_138 AS in1
     ON (in0.ISIN = in1.ISIN)

),

Summarize_140 AS (

  SELECT 
    SUM(`Weighted-Average`) AS `Weighted-Average`,
    `Currency Denomination` AS `Currency Denomination`,
    `Product Type` AS `Product Type`,
    Region AS Region,
    filename AS filename,
    ISIN AS ISIN
  
  FROM Join_136_inner AS in0
  
  GROUP BY 
    `Currency Denomination`, `Product Type`, Region, filename, ISIN

),

Formula_141_0 AS (

  SELECT 
    CAST('Upload' AS string) AS Action,
    CAST((CONCAT('Daily_trade_Volume_CEM', '_', filename)) AS string) AS Name,
    CAST('' AS string) AS CUSIP,
    *
  
  FROM Summarize_140 AS in0

),

AlteryxSelect_142 AS (

  SELECT 
    Action AS Action,
    ISIN AS ISIN,
    `Weighted-Average` AS `Weighted-Average`,
    `Product Type` AS `Product Type`,
    Region AS Region,
    Name AS Name,
    `Currency Denomination` AS `Currency Denomination`,
    * EXCEPT (`filename`, `Action`, `ISIN`, `Weighted-Average`, `Product Type`, `Region`, `Name`, `Currency Denomination`)
  
  FROM Formula_141_0 AS in0

)

SELECT *

FROM AlteryxSelect_142
