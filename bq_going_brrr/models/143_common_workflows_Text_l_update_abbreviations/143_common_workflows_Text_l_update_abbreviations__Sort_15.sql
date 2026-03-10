{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH uuidified_abbre_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('143_common_workflows_Text_l_update_abbreviations', 'uuidified_abbre_1') }}

),

Cleanse_20 AS (

  {{
    prophecy_basics.DataCleansing(
      ['uuidified_abbre_1'], 
      [
        { "name": "uuids", "dataType": "String" }, 
        { "name": "abbreviation_codes", "dataType": "String" }, 
        { "name": "abbreviated_phrases", "dataType": "String" }, 
        { "name": "referenced_object_type", "dataType": "String" }
      ], 
      'makeLowercase', 
      ['abbreviation_codes'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
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

TextInput_3 AS (

  SELECT * 
  
  FROM {{ ref('seed_3')}}

),

TextInput_3_cast AS (

  SELECT 
    CAST(abbreviation_codes AS STRING) AS abbreviation_codes,
    CAST(abbreviated_phrases AS STRING) AS abbreviated_phrases,
    CAST(UPDATE AS string) AS Update,
    CAST(referenced_object_type AS STRING) AS referenced_object_type,
    CAST(variableDelete AS STRING) AS variableDelete
  
  FROM TextInput_3 AS in0

),

Join_2_left AS (

  SELECT in0.*
  
  FROM TextInput_3_cast AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Cleanse_20 AS in1
    
    WHERE in1.abbreviation_codes IS NOT NULL
  ) AS in1_keys
     ON (in0.abbreviation_codes = in1_keys.abbreviation_codes)
  
  WHERE (in1_keys.abbreviation_codes IS NULL)

),

Join_2_inner AS (

  SELECT 
    in0.abbreviated_phrases AS new_abbreviation_phrases,
    in1.referenced_object_type AS referenced_object_type,
    in1.uuids AS uuids,
    in0.abbreviation_codes AS new_abbreviation_codes,
    in1.abbreviation_codes AS abbreviation_codes,
    in0.variableDelete AS variableDelete,
    in1.abbreviated_phrases AS abbreviated_phrases,
    IN0.UPDATE AS Update
  
  FROM TextInput_3_cast AS in0
  INNER JOIN Cleanse_20 AS in1
     ON (in0.abbreviation_codes = in1.abbreviation_codes)

),

Filter_11_reject_to_Filter_17 AS (

  SELECT * 
  
  FROM Join_2_inner AS in0
  
  WHERE (
          ((UPDATE <> 'X') OR ((UPDATE = 'X') IS NULL))
          AND ((VARIABLEDELETE <> 'X') OR ((VARIABLEDELETE = 'X') IS NULL))
        )

),

AlteryxSelect_13 AS (

  SELECT 
    abbreviation_codes AS abbreviation_codes,
    abbreviated_phrases AS abbreviated_phrases,
    uuids AS uuids
  
  FROM Filter_11_reject_to_Filter_17 AS in0

),

Filter_11 AS (

  SELECT * 
  
  FROM Join_2_inner AS in0
  
  WHERE (UPDATE = 'X')

),

AlteryxSelect_14 AS (

  SELECT 
    abbreviation_codes AS abbreviation_codes,
    new_abbreviation_phrases AS abbreviated_phrases,
    uuids AS uuids
  
  FROM Filter_11 AS in0

),

Union_12 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_13', 'AlteryxSelect_14'], 
      [
        '[{"name": "abbreviation_codes", "dataType": "String"}, {"name": "abbreviated_phrases", "dataType": "String"}, {"name": "uuids", "dataType": "String"}]', 
        '[{"name": "abbreviation_codes", "dataType": "String"}, {"name": "abbreviated_phrases", "dataType": "String"}, {"name": "uuids", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_4_0 AS (

  SELECT 
    CAST((GENERATE_UUID()) AS STRING) AS uuids,
    *
  
  FROM Join_2_left AS in0

),

Join_2_right AS (

  SELECT in0.*
  
  FROM Cleanse_20 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM TextInput_3_cast AS in1
    
    WHERE in1.abbreviation_codes IS NOT NULL
  ) AS in1_keys
     ON (in1_keys.abbreviation_codes = in0.abbreviation_codes)
  
  WHERE (in1_keys.abbreviation_codes IS NULL)

),

Union_5 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_2_right', 'Formula_4_0', 'Union_12'], 
      [
        '[{"name": "uuids", "dataType": "String"}, {"name": "abbreviation_codes", "dataType": "String"}, {"name": "abbreviated_phrases", "dataType": "String"}, {"name": "referenced_object_type", "dataType": "String"}]', 
        '[{"name": "uuids", "dataType": "String"}, {"name": "abbreviation_codes", "dataType": "String"}, {"name": "abbreviated_phrases", "dataType": "String"}, {"name": "Update", "dataType": "String"}, {"name": "referenced_object_type", "dataType": "String"}, {"name": "variableDelete", "dataType": "String"}]', 
        '[{"name": "abbreviation_codes", "dataType": "String"}, {"name": "abbreviated_phrases", "dataType": "String"}, {"name": "uuids", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_10 AS (

  SELECT * 
  
  FROM Union_5 AS in0
  
  WHERE (abbreviation_codes IS NOT NULL)

),

AlteryxSelect_19 AS (

  SELECT 
    uuids AS uuids,
    abbreviation_codes AS abbreviation_codes,
    abbreviated_phrases AS abbreviated_phrases,
    referenced_object_type AS referenced_object_type
  
  FROM Filter_10 AS in0

),

Sort_15 AS (

  SELECT * 
  
  FROM AlteryxSelect_19 AS in0
  
  ORDER BY abbreviation_codes ASC

)

SELECT *

FROM Sort_15
