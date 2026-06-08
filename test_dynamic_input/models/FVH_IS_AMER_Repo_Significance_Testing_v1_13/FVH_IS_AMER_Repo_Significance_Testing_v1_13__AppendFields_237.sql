{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_175 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Summarize_175')}}

),

DynamicInput_214 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing_v1_13', 'DynamicInput_214') }}

),

Join_179_left AS (

  SELECT in0.*
  
  FROM Summarize_175 AS in0
  ANTI JOIN DynamicInput_214 AS in1
     ON (in0.Curve = in1.Curve)

),

Formula_235_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Formula_235_0')}}

),

AppendFields_237 AS (

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
    `Adjusted Curve Risk`, 
    `Special ISIN Adjusted Risk`, 
    `MTM in reporting`, 
    `mtm`, 
    `Reporting`, 
    `Result`, 
    `Sig test output`),
    in1.*
  
  FROM Formula_235_0 AS in0
  INNER JOIN Join_179_left AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_237
