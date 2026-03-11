{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_46 AS (

  SELECT * 
  
  FROM {{ ref('seed_46')}}

),

TextInput_46_cast AS (

  SELECT CAST(Field1 AS STRING) AS Field1
  
  FROM TextInput_46 AS in0

),

TextToColumns_47 AS (

  {{
    prophecy_basics.TextToColumns(
      ['TextInput_46_cast'], 
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

TextToColumns_47_dropGem_0 AS (

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
  
  FROM TextToColumns_47 AS in0

),

Transpose_48_schemaTransform_0 AS (

  SELECT * EXCEPT (`Field1`)
  
  FROM TextToColumns_47_dropGem_0 AS in0

),

Transpose_48 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_48_schemaTransform_0'], 
      [], 
      ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], 
      'Name', 
      'Value', 
      ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'], 
      true
    )
  }}

),

RegEx_49 AS (

  {{
    prophecy_basics.Regex(
      ['Transpose_48'], 
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

AlteryxSelect_50 AS (

  SELECT 
    Name AS variableRow,
    * EXCEPT (`Value`, `Name`)
  
  FROM RegEx_49 AS in0

),

Transpose_51 AS (

  {{
    prophecy_basics.Transpose(
      ['AlteryxSelect_50'], 
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

Formula_52_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Name, 'Value', '')) AS STRING) AS Name,
    * EXCEPT (`name`)
  
  FROM Transpose_51 AS in0

),

AlteryxSelect_53 AS (

  SELECT 
    CAST(variableRow AS INT64) AS variableRow,
    CAST(Name AS INT64) AS Column,
    * EXCEPT (`variableRow`, `Name`)
  
  FROM Formula_52_0 AS in0

),

Filter_54 AS (

  SELECT * 
  
  FROM AlteryxSelect_53 AS in0
  
  WHERE (VALUE = 'A')

),

Formula_55_0 AS (

  SELECT 
    (variableRow - 1) AS Row2,
    (variableRow - 1) AS Row3,
    (variableRow + 1) AS Row4,
    (Column - 1) AS Col2,
    (Column + 1) AS Col3,
    (Column - 1) AS Col4,
    (variableRow + 1) AS Row5,
    (Column + 1) AS Col5,
    *
  
  FROM Filter_54 AS in0

),

Filter_56 AS (

  SELECT * 
  
  FROM Formula_55_0 AS in0
  
  WHERE (
          (
            (((Row2 > 0) AND (Row3 > 0)) AND ((Row4 > 0) AND (Row5 > 0)))
            AND (((Col2 > 0) AND (Col3 > 0)) AND ((Col4 > 0) AND (Col5 > 0)))
          )
          AND (
                (((Row2 < 11) AND (Row3 < 11)) AND ((Row4 < 11) AND (Row5 < 11)))
                AND (((Col2 < 11) AND (Col3 < 11)) AND ((Col4 < 11) AND (Col5 < 11)))
              )
        )

),

Join_57_inner AS (

  SELECT 
    in1.VALUE AS Value2,
    in0.*
  
  FROM Filter_56 AS in0
  INNER JOIN AlteryxSelect_53 AS in1
     ON ((in0.Row2 = in1.variableRow) AND (in0.Col2 = in1.Column))

),

Join_58_inner AS (

  SELECT 
    in1.VALUE AS Value3,
    in0.*
  
  FROM Join_57_inner AS in0
  INNER JOIN AlteryxSelect_53 AS in1
     ON ((in0.Row3 = in1.variableRow) AND (in0.Col3 = in1.Column))

),

Join_59_inner AS (

  SELECT 
    in1.VALUE AS Value4,
    in0.*
  
  FROM Join_58_inner AS in0
  INNER JOIN AlteryxSelect_53 AS in1
     ON ((in0.Row4 = in1.variableRow) AND (in0.Col4 = in1.Column))

),

Join_60_inner AS (

  SELECT 
    in1.VALUE AS Value5,
    in0.*
  
  FROM Join_59_inner AS in0
  INNER JOIN AlteryxSelect_53 AS in1
     ON ((in0.Row5 = in1.variableRow) AND (in0.Col5 = in1.Column))

),

Filter_61_to_Filter_62 AS (

  SELECT * 
  
  FROM Join_60_inner AS in0
  
  WHERE (
          (
            ((((Value2 = 'S') AND (Value5 = 'M')) OR (Value5 = 'S')) AND (Value2 = 'M'))
            AND (((Value3 = 'S') AND (Value4 = 'M')) OR (Value4 = 'S'))
          )
          AND (Value3 = 'M')
        )

),

Sort_64 AS (

  SELECT * 
  
  FROM Filter_61_to_Filter_62 AS in0
  
  ORDER BY variableRow ASC, Column ASC

)

SELECT *

FROM Sort_64
