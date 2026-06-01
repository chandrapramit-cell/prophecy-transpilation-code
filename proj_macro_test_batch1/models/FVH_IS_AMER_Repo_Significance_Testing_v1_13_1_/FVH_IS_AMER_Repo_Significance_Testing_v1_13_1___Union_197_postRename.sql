{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH DynamicInput_262 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1_', 'DynamicInput_262') }}

),

Summarize_60 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Summarize_60')}}

),

Unique_65 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Unique_65')}}

),

Join_30_inner AS (

  SELECT 
    in0.Region AS Region,
    in0.TradeID AS TradeID,
    in0.Sum_Impact AS Sum_Impact,
    in0.Portfolio AS Portfolio,
    in1.Valuation AS MTM,
    in0.* EXCEPT (`Region`, `TradeID`, `Sum_Impact`, `Portfolio`),
    in1.* EXCEPT (`Valuation`, `TradeId`, `TradeID`)
  
  FROM Summarize_60 AS in0
  INNER JOIN Unique_65 AS in1
     ON (in0.TradeID = in1.TradeID)

),

Formula_61_0 AS (

  SELECT 
    CAST(ABS(((Sum_Impact / MTM) * 100)) AS DOUBLE) AS SignificanceTest,
    *
  
  FROM Join_30_inner AS in0

),

Formula_61_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (ABS(SignificanceTest) > 5)
          THEN 'Level 3'
        ELSE 'Level 2'
      END
    ) AS string) AS Level,
    *
  
  FROM Formula_61_0 AS in0

),

Join_194_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Trade ID to be overriden`)
  
  FROM Formula_61_1 AS in0
  INNER JOIN DynamicInput_262 AS in1
     ON (in0.TradeID = in1.`Trade ID to be overriden`)

),

Formula_196_0 AS (

  SELECT 
    CAST(`Level to be overriden` AS string) AS Level,
    CAST('Y' AS string) AS `Override paranthesesOpenYslashNparanthesesClose`,
    * EXCEPT (`level`)
  
  FROM Join_194_inner AS in0

),

AlteryxSelect_198 AS (

  SELECT * EXCEPT (`Level to be overriden`)
  
  FROM Formula_196_0 AS in0

),

Union_197_0 AS (

  SELECT 
    CAST(MTM AS DOUBLE) AS prophecy_column_5,
    CAST(Region AS string) AS prophecy_column_1,
    CAST(SignificanceTest AS DOUBLE) AS prophecy_column_6,
    CAST(TradeID AS string) AS prophecy_column_2,
    CAST(Level AS string) AS prophecy_column_7,
    CAST(Sum_Impact AS DOUBLE) AS prophecy_column_3,
    CAST(`Override paranthesesOpenYslashNparanthesesClose` AS string) AS prophecy_column_8,
    CAST(Portfolio AS string) AS prophecy_column_4
  
  FROM AlteryxSelect_198 AS in0

),

Join_194_left AS (

  SELECT in0.*
  
  FROM Formula_61_1 AS in0
  ANTI JOIN DynamicInput_262 AS in1
     ON (in0.TradeID = in1.`Trade ID to be overriden`)

),

Formula_263_0 AS (

  SELECT 
    CAST('N' AS string) AS `Override paranthesesOpenYslashNparanthesesClose`,
    *
  
  FROM Join_194_left AS in0

),

Union_197_1 AS (

  SELECT 
    CAST(MTM AS DOUBLE) AS prophecy_column_5,
    CAST(Region AS string) AS prophecy_column_1,
    CAST(SignificanceTest AS DOUBLE) AS prophecy_column_6,
    CAST(TradeID AS string) AS prophecy_column_2,
    CAST(Level AS string) AS prophecy_column_7,
    CAST(Sum_Impact AS DOUBLE) AS prophecy_column_3,
    CAST(`Override paranthesesOpenYslashNparanthesesClose` AS string) AS prophecy_column_8,
    CAST(Portfolio AS string) AS prophecy_column_4
  
  FROM Formula_263_0 AS in0

),

Union_197 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_197_0', 'Union_197_1'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "Double"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "Double"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_197_postRename AS (

  SELECT 
    prophecy_column_7 AS Level,
    prophecy_column_6 AS SignificanceTest,
    prophecy_column_5 AS MTM,
    prophecy_column_3 AS Sum_Impact,
    prophecy_column_8 AS `Override paranthesesOpenYslashNparanthesesClose`,
    prophecy_column_1 AS Region,
    prophecy_column_4 AS Portfolio,
    prophecy_column_2 AS TradeID
  
  FROM Union_197 AS in0

)

SELECT *

FROM Union_197_postRename
