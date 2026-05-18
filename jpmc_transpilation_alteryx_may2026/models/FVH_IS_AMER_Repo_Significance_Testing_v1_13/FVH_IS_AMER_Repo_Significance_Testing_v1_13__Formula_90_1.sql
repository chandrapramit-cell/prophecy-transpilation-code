{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_170_to_Formula_59_2 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Formula_170_to_Formula_59_2')}}

),

Summarize_92 AS (

  SELECT 
    DISTINCT TradeID AS TradeID,
    Curve AS Curve
  
  FROM Formula_170_to_Formula_59_2 AS in0

),

Union_197_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Union_197_postRename')}}

),

Join_91_inner AS (

  SELECT 
    in0.Portfolio AS Portfolio,
    in1.Curve AS Curve,
    in0.TradeID AS TradeID,
    in0.Region AS Region,
    in0.MTM AS MTM,
    in0.Level AS Level
  
  FROM Union_197_postRename AS in0
  INNER JOIN Summarize_92 AS in1
     ON (in0.TradeID = in1.TradeID)

),

Formula_86_0 AS (

  SELECT 
    CAST(1 AS DOUBLE) AS `MTM Ratio`,
    CAST((CONCAT(TradeID, Curve)) AS string) AS ConcatFind,
    CAST((
      CASE
        WHEN CAST((coalesce((CONTAINS(LOWER(Portfolio), LOWER('FPA'))), FALSE)) AS BOOLEAN)
          THEN 'FPA'
        ELSE 'Repo'
      END
    ) AS string) AS Product,
    *
  
  FROM Join_91_inner AS in0

),

DynamicInput_215 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing_v1_13', 'DynamicInput_215') }}

),

FindReplace_87_allRules AS (

  SELECT collect_list(struct(Ratio AS Ratio, Concat AS Concat)) AS _rules
  
  FROM DynamicInput_215 AS in0

),

FindReplace_87_join AS (

  SELECT 
    in0.Portfolio AS Portfolio,
    in0.Curve AS Curve,
    in0.TradeID AS TradeID,
    in1._rules AS _rules,
    in0.Region AS Region,
    in0.ConcatFind AS ConcatFind,
    in0.MTM AS MTM,
    in0.`MTM Ratio` AS `MTM Ratio`,
    in0.Product AS Product,
    in0.Level AS Level
  
  FROM Formula_86_0 AS in0
  FULL JOIN FindReplace_87_allRules AS in1
     ON TRUE

),

FindReplace_87_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(
              regexp_extract(`ConcatFind`, concat('^', concat('(?<=^|\\s)', rule['Concat'], '(?=\\s|$)'), '$'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_87_join AS in0

),

FindReplace_87_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.Ratio')) AS Ratio,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_87_0 AS in0

),

Cleanse_89 AS (

  {{
    prophecy_basics.DataCleansing(
      ['FindReplace_87_reorg_0'], 
      [
        { "name": "Ratio", "dataType": "String" }, 
        { "name": "Portfolio", "dataType": "String" }, 
        { "name": "Curve", "dataType": "String" }, 
        { "name": "TradeID", "dataType": "String" }, 
        { "name": "Region", "dataType": "String" }, 
        { "name": "ConcatFind", "dataType": "String" }, 
        { "name": "MTM", "dataType": "Double" }, 
        { "name": "MTM Ratio", "dataType": "Double" }, 
        { "name": "Product", "dataType": "String" }, 
        { "name": "Level", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['Ratio'], 
      false, 
      '', 
      true, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Formula_90_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (CAST(Ratio AS INTEGER) = 0)
          THEN MTM
        ELSE (MTM * CAST(Ratio AS DOUBLE))
      END
    ) AS DOUBLE) AS MTM,
    * EXCEPT (`mtm`)
  
  FROM Cleanse_89 AS in0

),

Formula_90_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (MTM >= 0)
          THEN 'Asset'
        ELSE 'Liability'
      END
    ) AS string) AS `Asset Or Liability`,
    *
  
  FROM Formula_90_0 AS in0

)

SELECT *

FROM Formula_90_1
