{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_151 AS (

  SELECT * 
  
  FROM {{ ref('seed_151')}}

),

TextInput_151_cast AS (

  SELECT 
    CAST(`Data Type` AS STRING) AS `Data Type`,
    CAST(`File Format` AS STRING) AS `File Format`
  
  FROM TextInput_151 AS in0

),

FindReplace_152_allRules AS (

  SELECT ARRAY_AGG(struct(`Data Type` AS `Data Type`, `File Format` AS `File Format`)) AS _rules
  
  FROM TextInput_151_cast AS in0

),

Query__Sheet1___143 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DQ_Macro', 'Query__Sheet1___143') }}

),

AlteryxSelect_153 AS (

  SELECT FileName AS FileName
  
  FROM Query__Sheet1___143 AS in0

),

Query__Sheet1___144 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DQ_Macro', 'Query__Sheet1___144') }}

),

AlteryxSelect_149 AS (

  SELECT FileName AS FileName
  
  FROM Query__Sheet1___144 AS in0

),

FindReplace_152_join AS (

  SELECT 
    in0.FileName AS FileName,
    in1.`_rules` AS _rules
  
  FROM AlteryxSelect_149 AS in0
  FULL JOIN FindReplace_152_allRules AS in1
     ON TRUE

),

FindReplace_152_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract(`FileName`, rule['File Format'], 0)) > 0), 1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_152_join AS in0

),

FindReplace_152_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(`_extracted_rule`, '$.Data Type')) AS `Data Type`,
    (GET_JSON_OBJECT(`_extracted_rule`, '$.File Format')) AS `File Format`,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_152_0 AS in0

),

AlteryxSelect_161 AS (

  SELECT 
    `Data Type` AS `Actual Data Type`,
    `File Format` AS `Actual File Format`
  
  FROM FindReplace_152_reorg_0 AS in0

),

Join_162_inner_rightRecordPosition AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin`
  
  FROM AlteryxSelect_161

),

FindReplace_154_allRules AS (

  SELECT ARRAY_AGG(struct(`Data Type` AS `Data Type`, `File Format` AS `File Format`)) AS _rules
  
  FROM TextInput_151_cast AS in0

),

FindReplace_154_join AS (

  SELECT 
    in0.FileName AS FileName,
    in1.`_rules` AS _rules
  
  FROM AlteryxSelect_153 AS in0
  FULL JOIN FindReplace_154_allRules AS in1
     ON TRUE

),

FindReplace_154_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract(`FileName`, rule['File Format'], 0)) > 0), 1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_154_join AS in0

),

FindReplace_154_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(`_extracted_rule`, '$.Data Type')) AS `Data Type`,
    (GET_JSON_OBJECT(`_extracted_rule`, '$.File Format')) AS `File Format`,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_154_0 AS in0

),

AlteryxSelect_160 AS (

  SELECT 
    `Data Type` AS `Expected Data Type`,
    `File Format` AS `Expected File Format`
  
  FROM FindReplace_154_reorg_0 AS in0

),

Join_162_inner_leftRecordPosition AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin`
  
  FROM AlteryxSelect_160

),

Join_162_inner AS (

  SELECT 
    in0.* EXCEPT (`recordPositionForJoin`),
    in1.* EXCEPT (`recordPositionForJoin`)
  
  FROM Join_162_inner_leftRecordPosition AS in0
  INNER JOIN Join_162_inner_rightRecordPosition AS in1
     ON (in0.recordPositionForJoin = in1.recordPositionForJoin)

),

Formula_163_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`Expected File Format` = `Actual File Format`)
          THEN 'Yes'
        ELSE 'No '
      END
    ) AS STRING) AS `Same File Types`,
    *
  
  FROM Join_162_inner AS in0

)

SELECT *

FROM Formula_163_0
