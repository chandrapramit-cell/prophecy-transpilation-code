{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_36_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_36_postRename')}}

),

Formula_235_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_235_0')}}

),

Summarize_126 AS (

  SELECT 
    DISTINCT `Source System` AS `Source System`,
    `Risk Type` AS `Risk Type`,
    CURVE_TYPE AS CURVE_TYPE
  
  FROM Union_36_postRename AS in0

),

AppendFields_228 AS (

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
    `TradeCurveList`, 
    `MTM Mismatch`, 
    `VA Curve to Map`, 
    `Missing Threshold`, 
    `Missing MTM`, 
    `Notional Ratio Missing`, 
    `Significance Test Missed Trades`, 
    `Adjusted Curve Risk`, 
    `Special ISIN Adjusted Risk`, 
    `Check Totem Tested Curve`, 
    `MTM in reporting`, 
    `mtm`, 
    `Reporting`, 
    `Result`, 
    `Sig test output`),
    in1.*
  
  FROM Formula_235_0 AS in0
  INNER JOIN Summarize_126 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_228
