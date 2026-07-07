{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_203 AS (

  SELECT * 
  
  FROM {{ ref('seed_FVH_IS_AMER_Repo_Significance_Testing_203')}}

),

TextInput_203_cast AS (

  SELECT 
    CAST(Field1 AS string) AS Field1,
    CAST(RawPath AS string) AS RawPath,
    CAST(` TabName` AS string) AS ` TabName`,
    CAST(`Output Raw Path` AS string) AS `Output Raw Path`
  
  FROM TextInput_203 AS in0

),

Formula_204_0 AS (

  SELECT 
    CAST((ELEMENT_AT((SPLIT(Field1, '\\s+')), CAST(2 AS INTEGER))) AS string) AS Month,
    *
  
  FROM TextInput_203_cast AS in0

),

Formula_204_1 AS (

  SELECT 
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\Totem Tested.xlsx|||Sheet1')) AS string) AS `Totem Tested`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\Backtested Trades.xlsx|||Sheet1')) AS string) AS `Backtested Trades`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\TOTEM Map.xlsx|||Sheet1')) AS string) AS `TOTEM Map`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\Fvo trades vcg.xlsx|||Sheet1')) AS string) AS `Fvo trades vcg`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\GEM PNL Repo Dashboard.xlsx|||Athena Data 1')) AS string) AS `GEM PNL Repo Dashboard`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\AMER MTD PNL Repo Dashboard.xlsx|||Athena Data 1')) AS string) AS `AMER MTD PNL Repo Dashboard`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\PLATO_UCN_Data.xlsx|||PLATO_UCN_Data')) AS string) AS PLATO_UCN_Data,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\OfficialTRisk_FIF_EM_DL.csv')) AS string) AS OfficialTRisk_FIF_EM_DL,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\OfficialTRisk_NA - Repos DL.csv')) AS string) AS `OfficialTRisk_NA - Repos DL`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\Athena FVO.csv')) AS string) AS `Athena FVO`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\MTM Ratio.xlsx|||Sheet1')) AS string) AS `MTM Ratio`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\CurveMap.xlsx|||CurveMap')) AS string) AS CurveMap,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\Charges.xlsx|||Sheet1')) AS string) AS Charges,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\Exclusions.xlsx|||Sheet1')) AS string) AS Exclusions,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\Exclusions.xlsx|||Curve Exclusions')) AS string) AS `Curve Exclusions`,
    CAST((CONCAT((CONCAT(RawPath, MONTH)), '\\Totem RepoSpecials.xlsx|||Sheet1')) AS string) AS `Totem RepoSpecials`,
    CAST('Additional Field' AS string) AS `Additional Field`,
    *
  
  FROM Formula_204_0 AS in0

)

SELECT *

FROM Formula_204_1
