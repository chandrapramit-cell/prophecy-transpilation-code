{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_1')}}

),

TextInput_1_cast AS (

  SELECT CAST(Field1 AS STRING) AS Field1
  
  FROM TextInput_1 AS in0

),

TextToColumns_4 AS (

  {{
    prophecy_basics.TextToColumns(
      ['TextInput_1_cast'], 
      'Field1', 
      "\\\\\\\\\\\\n", 
      'splitColumns', 
      10, 
      'leaveExtraCharLastCol', 
      'Field1', 
      'Field1', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_4_dropGem_0 AS (

  SELECT 
    Field1_1_Field1 AS `1`,
    Field1_2_Field1 AS `2`,
    Field1_3_Field1 AS `3`,
    Field1_4_Field1 AS `4`,
    Field1_5_Field1 AS `5`,
    Field1_6_Field1 AS `6`,
    Field1_7_Field1 AS `7`,
    Field1_8_Field1 AS `8`,
    Field1_9_Field1 AS `9`,
    Field1_10_Field1 AS `10`,
    * EXCEPT (`Field1_1_Field1`, 
    `Field1_2_Field1`, 
    `Field1_3_Field1`, 
    `Field1_4_Field1`, 
    `Field1_5_Field1`, 
    `Field1_6_Field1`, 
    `Field1_7_Field1`, 
    `Field1_8_Field1`, 
    `Field1_9_Field1`, 
    `Field1_10_Field1`)
  
  FROM TextToColumns_4 AS in0

),

Transpose_5_schemaTransform_0 AS (

  SELECT * EXCEPT (`Field1`)
  
  FROM TextToColumns_4_dropGem_0 AS in0

),

Transpose_5 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_5_schemaTransform_0'], 
      [], 
      ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], 
      'Name', 
      'Value', 
      ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], 
      true
    )
  }}

),

RegEx_7 AS (

  {{
    prophecy_basics.Regex(
      ['Transpose_5'], 
      [], 
      '[{"name": "Name", "dataType": "String"}, {"name": "Value", "dataType": "String"}]', 
      'Value', 
      '(\w)', 
      'tokenize', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      10, 
      'dropExtraWithoutWarning', 
      'Value', 
      '', 
      false
    )
  }}

),

AlteryxSelect_8 AS (

  SELECT 
    Name AS variableRow,
    * EXCEPT (`Value`, `Name`)
  
  FROM RegEx_7 AS in0

),

Transpose_9 AS (

  {{
    prophecy_basics.Transpose(
      ['AlteryxSelect_8'], 
      ['variableRow'], 
      ['Value1', 'Value2', 'Value3', 'Value4', 'Value5', 'Value6', 'Value7', 'Value8', 'Value9', 'Value10'], 
      'Name', 
      'Value', 
      [
        'Value7', 
        'Value4', 
        'Value8', 
        'Value5', 
        'variableRow', 
        'Value10', 
        'Value2', 
        'Value9', 
        'Value3', 
        'Value6', 
        'Value1'
      ], 
      true
    )
  }}

),

Formula_10_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Name, 'Value', '')) AS STRING) AS Name,
    * EXCEPT (`name`)
  
  FROM Transpose_9 AS in0

),

AlteryxSelect_11 AS (

  SELECT 
    CAST(variableRow AS INT64) AS variableRow,
    CAST(Name AS INT64) AS Column,
    * EXCEPT (`variableRow`, `Name`)
  
  FROM Formula_10_0 AS in0

)

SELECT *

FROM AlteryxSelect_11
