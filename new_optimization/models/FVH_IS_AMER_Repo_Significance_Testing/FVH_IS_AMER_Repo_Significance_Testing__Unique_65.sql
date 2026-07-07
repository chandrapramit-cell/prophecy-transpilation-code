{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH DynamicInput_219 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing', 'DynamicInput_219') }}

),

AlteryxSelect_67 AS (

  SELECT 
    discounted_npv AS discounted_npv,
    sign AS sign,
    start_cash AS start_cash,
    TradeId AS TradeId
  
  FROM DynamicInput_219 AS in0

),

DynamicInput_218 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing', 'DynamicInput_218') }}

),

AlteryxSelect_153 AS (

  SELECT 
    discounted_npv AS discounted_npv,
    sign AS sign,
    start_cash AS start_cash,
    TradeId AS TradeId
  
  FROM DynamicInput_218 AS in0

),

DynamicInput_208 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing', 'DynamicInput_208') }}

),

AlteryxSelect_25 AS (

  SELECT 
    F1 AS F1,
    MTM AS MTM
  
  FROM DynamicInput_208 AS in0

),

Union_26_1 AS (

  SELECT 
    CAST(F1 AS string) AS prophecy_column_1,
    CAST(MTM AS DOUBLE) AS prophecy_column_2
  
  FROM AlteryxSelect_25 AS in0

),

Formula_66_0 AS (

  SELECT 
    CAST(((start_cash * sign) + discounted_npv) AS DOUBLE) AS MTM,
    *
  
  FROM AlteryxSelect_67 AS in0

),

AlteryxSelect_20 AS (

  SELECT 
    TradeId AS TradeId,
    MTM AS MTM
  
  FROM Formula_66_0 AS in0

),

Union_26_0 AS (

  SELECT 
    CAST(TradeId AS string) AS prophecy_column_1,
    CAST(MTM AS DOUBLE) AS prophecy_column_2
  
  FROM AlteryxSelect_20 AS in0

),

Formula_154_0 AS (

  SELECT 
    CAST(((start_cash * sign) + discounted_npv) AS DOUBLE) AS MTM,
    *
  
  FROM AlteryxSelect_153 AS in0

),

AlteryxSelect_155 AS (

  SELECT 
    TradeId AS TradeId,
    MTM AS MTM
  
  FROM Formula_154_0 AS in0

),

Union_26_3 AS (

  SELECT 
    CAST(TradeId AS string) AS prophecy_column_1,
    CAST(MTM AS DOUBLE) AS prophecy_column_2
  
  FROM AlteryxSelect_155 AS in0

),

DynamicInput_217 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing', 'DynamicInput_217') }}

),

AlteryxSelect_64 AS (

  SELECT 
    TradeID AS TradeID,
    CAST(Valuation AS DOUBLE) AS Valuation
  
  FROM DynamicInput_217 AS in0

),

Union_26_2 AS (

  SELECT 
    CAST(TradeID AS string) AS prophecy_column_1,
    CAST(Valuation AS DOUBLE) AS prophecy_column_2
  
  FROM AlteryxSelect_64 AS in0

),

Union_26 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_26_0', 'Union_26_1', 'Union_26_2', 'Union_26_3'], 
      [
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "Double"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "Double"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "Double"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_26_postRename AS (

  SELECT 
    prophecy_column_1 AS TradeID,
    prophecy_column_2 AS Valuation
  
  FROM Union_26 AS in0

),

Unique_65 AS (

  SELECT * 
  
  FROM Union_26_postRename AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY TradeID ORDER BY TradeID) = 1

)

SELECT *

FROM Unique_65
