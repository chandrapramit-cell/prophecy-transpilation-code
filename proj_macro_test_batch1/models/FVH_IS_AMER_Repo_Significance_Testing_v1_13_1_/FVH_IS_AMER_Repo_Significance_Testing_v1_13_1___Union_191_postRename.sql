{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_175 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Summarize_175')}}

),

DynamicInput_214 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1_', 'DynamicInput_214') }}

),

Join_179_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Curve`)
  
  FROM Summarize_175 AS in0
  INNER JOIN DynamicInput_214 AS in1
     ON (in0.Curve = in1.Curve)

),

TextInput_51_cast AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___TextInput_51_cast')}}

),

Join_180_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Tenor`, `VA Map`)
  
  FROM Join_179_inner AS in0
  INNER JOIN TextInput_51_cast AS in1
     ON (in0.Tenor = in1.Tenor)

),

Summarize_182 AS (

  SELECT 
    MAX(`Tenor Map`) AS `Max_Tenor Map`,
    TradeID AS TradeID,
    Curve AS Curve,
    Portfolio AS Portfolio,
    `Totem Curve` AS `Totem Curve`
  
  FROM Join_180_inner AS in0
  
  GROUP BY 
    TradeID, Curve, Portfolio, `Totem Curve`

),

DynamicInput_220 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1_', 'DynamicInput_220') }}

),

Join_185_inner AS (

  SELECT 
    in1.`Totem Curve` AS `Right_Totem Curve`,
    in0.*,
    in1.* EXCEPT (`Totem Curve`)
  
  FROM Summarize_182 AS in0
  INNER JOIN DynamicInput_220 AS in1
     ON (in0.`Totem Curve` = in1.`Totem Curve`)

),

DynamicInput_209 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1_', 'DynamicInput_209') }}

),

Formula_184_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((coalesce((CONTAINS(LOWER(Portfolio), LOWER('FPA'))), FALSE)) AS BOOLEAN)
          THEN 'FPA'
        ELSE 'Repo'
      END
    ) AS string) AS PortfolioType,
    *
  
  FROM Join_185_inner AS in0

),

Union_191_1 AS (

  SELECT CAST(TradeID AS string) AS prophecy_column_1
  
  FROM DynamicInput_209 AS in0

),

Formula_184_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (PortfolioType = 'FPA')
          THEN 'No'
        WHEN (`Max_Tenor Map` > 18)
          THEN 'No'
        ELSE 'Yes'
      END
    ) AS string) AS `Backtested Trades`,
    *
  
  FROM Formula_184_0 AS in0

),

Filter_188 AS (

  SELECT * 
  
  FROM Formula_184_1 AS in0
  
  WHERE (`Backtested Trades` = 'Yes')

),

Union_191_0 AS (

  SELECT 
    CAST(`Max_Tenor Map` AS INTEGER) AS prophecy_column_5,
    CAST(TradeID AS string) AS prophecy_column_1,
    CAST(`Right_Totem Curve` AS string) AS prophecy_column_6,
    CAST(Curve AS string) AS prophecy_column_2,
    CAST(PortfolioType AS string) AS prophecy_column_7,
    CAST(Portfolio AS string) AS prophecy_column_3,
    CAST(`Backtested Trades` AS string) AS prophecy_column_8,
    CAST(`Totem Curve` AS string) AS prophecy_column_4
  
  FROM Filter_188 AS in0

),

Union_191 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_191_0', 'Union_191_1'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "Integer"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_191_postRename AS (

  SELECT 
    prophecy_column_3 AS Portfolio,
    prophecy_column_8 AS `Backtested Trades`,
    prophecy_column_2 AS Curve,
    prophecy_column_5 AS `Max_Tenor Map`,
    prophecy_column_7 AS PortfolioType,
    prophecy_column_6 AS `Right_Totem Curve`,
    prophecy_column_4 AS `Totem Curve`,
    prophecy_column_1 AS TradeID
  
  FROM Union_191 AS in0

)

SELECT *

FROM Union_191_postRename
