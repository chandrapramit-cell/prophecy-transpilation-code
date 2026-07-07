{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_235_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Formula_235_0')}}

),

Formula_31_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Formula_31_0')}}

),

AppendFields_223 AS (

  SELECT 
    in0.* EXCEPT (`Field1`, 
    `RawPath`, 
    ` TabName`, 
    `Output Raw Path`, 
    `Month`, 
    `Totem Tested`, 
    `Backtested Trades`, 
    `TOTEM Map`, 
    `Fvo trades vcg`, 
    `GEM PNL Repo Dashboard`, 
    `AMER MTD PNL Repo Dashboard`, 
    `PLATO_UCN_Data`, 
    `OfficialTRisk_FIF_EM_DL`, 
    `OfficialTRisk_NA - Repos DL`, 
    `Athena FVO`, 
    `MTM Ratio`, 
    `CurveMap`, 
    `Charges`, 
    `Exclusions`, 
    `Curve Exclusions`, 
    `Totem RepoSpecials`, 
    `Additional Field`, 
    `Included SS CurveType Risk Type`, 
    `TradeCurveList`, 
    `MTM Mismatch`, 
    `VA Curve to Map`, 
    `Missing Threshold`, 
    `Missing MTM`, 
    `Notional Ratio Missing`, 
    `Significance Test Missed Trades`, 
    `Special ISIN Adjusted Risk`, 
    `Check Totem Tested Curve`, 
    `MTM in reporting`, 
    `mtm`, 
    `Reporting`, 
    `Result`, 
    `Sig test output`),
    in1.*
  
  FROM Formula_235_0 AS in0
  INNER JOIN Formula_31_0 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_223
