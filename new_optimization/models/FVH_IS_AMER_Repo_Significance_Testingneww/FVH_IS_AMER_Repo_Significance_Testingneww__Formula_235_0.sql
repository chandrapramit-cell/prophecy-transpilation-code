{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_204_1 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testingneww__Formula_204_1')}}

),

Formula_235_0 AS (

  SELECT 
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Checks.xlsx|||Included SS CurveType Risk Type')) AS string) AS `Included SS CurveType Risk Type`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Checks.xlsx|||TradeCurveList')) AS string) AS TradeCurveList,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Checks.xlsx|||MTM Mismatch')) AS string) AS `MTM Mismatch`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Checks.xlsx|||VA Curve to Map')) AS string) AS `VA Curve to Map`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Checks.xlsx|||Missing Threshold')) AS string) AS `Missing Threshold`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Checks.xlsx|||Missing MTM')) AS string) AS `Missing MTM`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Checks.xlsx|||Notional Ratio Missing')) AS string) AS `Notional Ratio Missing`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Checks.xlsx|||Significance Test Missed Trades')) AS string) AS `Significance Test Missed Trades`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\FilteredRisk.xlsx|||Adjusted Curve Risk')) AS string) AS `Adjusted Curve Risk`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\FilteredRisk.xlsx|||Special ISIN Adjusted Risk')) AS string) AS `Special ISIN Adjusted Risk`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\FilteredRisk.xlsx|||Check Totem Tested Curve')) AS string) AS `Check Totem Tested Curve`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\MTM in reporting.xlsx|||Sheet1')) AS string) AS `MTM in reporting`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\mtm.xlsx|||Sheet1')) AS string) AS mtm,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Reporting.xlsx|||Reporting')) AS string) AS Reporting,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Result.xlsx|||Sheet1')) AS string) AS Result,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Sig test output.xlsx|||Sheet1')) AS string) AS `Sig test output`,
    CAST((CONCAT((CONCAT(`Output Raw Path`, MONTH)), '\\Trades.xlsx|||Backtested Trades')) AS string) AS `Backtested Trades`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\Levelling Overrides.xlsx|||Overrides')) AS string) AS Overrides,
    * EXCEPT (`backtested trades`)
  
  FROM Formula_204_1 AS in0

)

SELECT *

FROM Formula_235_0
