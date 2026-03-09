{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_6 AS (

  SELECT * 
  
  FROM {{ ref('seed_6')}}

),

TextInput_6_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_6 AS in0

),

AlteryxSelect_22 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_6_cast AS in0

),

RecordID_13 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM AlteryxSelect_22

),

DynamicSelect_55 AS (

  SELECT *
  
  FROM {{ ref('Runner_App_Generator__DynamicSelect_55')}}

),

AppendFields_34 AS (

  SELECT 
    in0.AYX_RecordID AS NodeCount,
    in0.* EXCEPT (`AYX_RecordID`),
    in1.*
  
  FROM DynamicSelect_55 AS in0
  INNER JOIN RecordID_13 AS in1
     ON TRUE

),

TextInput_5 AS (

  SELECT * 
  
  FROM {{ ref('seed_5')}}

),

TextInput_5_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_5 AS in0

),

AlteryxSelect_21 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_5_cast AS in0

),

RecordID_12 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM AlteryxSelect_21

),

TextInput_3 AS (

  SELECT * 
  
  FROM {{ ref('seed_3')}}

),

TextInput_3_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_3 AS in0

),

AlteryxSelect_25 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_3_cast AS in0

),

RecordID_16 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM AlteryxSelect_25

),

AppendFields_43 AS (

  SELECT 
    in0.AYX_RecordID AS NodeCount,
    in0.* EXCEPT (`AYX_RecordID`),
    in1.*
  
  FROM DynamicSelect_55 AS in0
  INNER JOIN RecordID_16 AS in1
     ON TRUE

),

Sort_44 AS (

  SELECT * 
  
  FROM AppendFields_43 AS in0
  
  ORDER BY WorkflowFullPath ASC, RecordID ASC

),

Formula_45_0 AS (

  SELECT 
    (8 + ((NodeCount - 1) * 6)) AS NodeID,
    (54 + ((NodeCount - 1) * 192)) AS PositionY,
    CAST((REGEXP_REPLACE(Field_1, 'AYXModuleFullPathToReplace', WorkflowFullPath)) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Sort_44 AS in0

),

Formula_45_1 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Node ToolID=")([0-9]+)(?=">)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(NodeID AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_45_0 AS in0

),

TextInput_7 AS (

  SELECT * 
  
  FROM {{ ref('seed_7')}}

),

TextInput_7_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_7 AS in0

),

AlteryxSelect_23 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_7_cast AS in0

),

RecordID_14 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM AlteryxSelect_23

),

AppendFields_37 AS (

  SELECT 
    in0.AYX_RecordID AS NodeCount,
    in0.* EXCEPT (`AYX_RecordID`),
    in1.*
  
  FROM DynamicSelect_55 AS in0
  INNER JOIN RecordID_14 AS in1
     ON TRUE

),

Sort_38 AS (

  SELECT * 
  
  FROM AppendFields_37 AS in0
  
  ORDER BY WorkflowFullPath ASC, RecordID ASC

),

Formula_39_0 AS (

  SELECT 
    (6 + ((NodeCount - 1) * 6)) AS NodeID,
    (54 + ((NodeCount - 1) * 192)) AS PositionY,
    CAST((REGEXP_REPLACE(Field_1, 'AYXModuleFullPathToReplace', WorkflowFullPath)) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Sort_38 AS in0

),

Formula_39_1 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Node ToolID=")([0-9]+)(?=">)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(NodeID AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_39_0 AS in0

),

TextInput_4 AS (

  SELECT * 
  
  FROM {{ ref('seed_4')}}

),

AppendFields_31 AS (

  SELECT 
    in0.AYX_RecordID AS NodeCount,
    in0.* EXCEPT (`AYX_RecordID`),
    in1.*
  
  FROM DynamicSelect_55 AS in0
  INNER JOIN RecordID_12 AS in1
     ON TRUE

),

Sort_32 AS (

  SELECT * 
  
  FROM AppendFields_31 AS in0
  
  ORDER BY WorkflowFullPath ASC, RecordID ASC

),

Formula_33_0 AS (

  SELECT 
    (4 + ((NodeCount - 1) * 6)) AS NodeID,
    (54 + ((NodeCount - 1) * 192)) AS PositionY,
    CAST((REGEXP_REPLACE(Field_1, 'AYXModuleFullPathToReplace', WorkflowFullPath)) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Sort_32 AS in0

),

Formula_33_1 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Node ToolID=")([0-9]+)(?=">)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(NodeID AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_33_0 AS in0

),

TextInput_2 AS (

  SELECT * 
  
  FROM {{ ref('seed_2')}}

),

TextInput_2_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_2 AS in0

),

AlteryxSelect_24 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_2_cast AS in0

),

RecordID_15 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM AlteryxSelect_24

),

AppendFields_40 AS (

  SELECT 
    in0.AYX_RecordID AS NodeCount,
    in0.* EXCEPT (`AYX_RecordID`),
    in1.*
  
  FROM DynamicSelect_55 AS in0
  INNER JOIN RecordID_15 AS in1
     ON TRUE

),

TextInput_4_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_4 AS in0

),

AlteryxSelect_20 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_4_cast AS in0

),

RecordID_11 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM AlteryxSelect_20

),

AppendFields_18 AS (

  SELECT 
    in0.AYX_RecordID AS NodeCount,
    in0.* EXCEPT (`AYX_RecordID`),
    in1.*
  
  FROM DynamicSelect_55 AS in0
  INNER JOIN RecordID_11 AS in1
     ON TRUE

),

Sort_29 AS (

  SELECT * 
  
  FROM AppendFields_18 AS in0
  
  ORDER BY WorkflowFullPath ASC, RecordID ASC

),

Formula_30_0 AS (

  SELECT 
    (3 + ((NodeCount - 1) * 6)) AS NodeID,
    (54 + ((NodeCount - 1) * 192)) AS PositionY,
    CAST((REGEXP_REPLACE(Field_1, 'AYXModuleFullPathToReplace', WorkflowFullPath)) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Sort_29 AS in0

),

Sort_35 AS (

  SELECT * 
  
  FROM AppendFields_34 AS in0
  
  ORDER BY WorkflowFullPath ASC, RecordID ASC

),

Formula_36_0 AS (

  SELECT 
    (5 + ((NodeCount - 1) * 6)) AS NodeID,
    (54 + ((NodeCount - 1) * 192)) AS PositionY,
    CAST((REGEXP_REPLACE(Field_1, 'AYXModuleFullPathToReplace', WorkflowFullPath)) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Sort_35 AS in0

),

Formula_36_1 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Node ToolID=")([0-9]+)(?=">)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(NodeID AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_36_0 AS in0

),

Formula_39_2 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Position x="462" y=")([0-9]+)(?=" \\/>)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(PositionY AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_39_1 AS in0

),

Formula_30_1 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Node ToolID=")([0-9]+)(?=">)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(NodeID AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_30_0 AS in0

),

Formula_30_2 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Position x="54" y=")([0-9]+)(?=" \\/>)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(PositionY AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_30_1 AS in0

),

Formula_45_2 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Position x="750" y=")([0-9]+)(?=" \\/>)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(PositionY AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_45_1 AS in0

),

Formula_33_2 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Position x="186" y=")([0-9]+)(?=" \\/>)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(PositionY AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_33_1 AS in0

),

Formula_36_2 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Position x="318" y=")([0-9]+)(?=" \\/>)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(PositionY AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_36_1 AS in0

),

Sort_41 AS (

  SELECT * 
  
  FROM AppendFields_40 AS in0
  
  ORDER BY WorkflowFullPath ASC, RecordID ASC

),

Formula_42_0 AS (

  SELECT 
    (7 + ((NodeCount - 1) * 6)) AS NodeID,
    (54 + ((NodeCount - 1) * 192)) AS PositionY,
    CAST((REGEXP_REPLACE(Field_1, 'AYXModuleFullPathToReplace', WorkflowFullPath)) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Sort_41 AS in0

),

Formula_42_1 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Node ToolID=")([0-9]+)(?=">)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(NodeID AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_42_0 AS in0

),

Formula_42_2 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=<Position x="606" y=")([0-9]+)(?=" \\/>)', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(PositionY AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_42_1 AS in0

),

Union_49 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_39_2', 'Formula_30_2', 'Formula_42_2', 'Formula_36_2', 'Formula_45_2', 'Formula_33_2'], 
      [
        '[{"name": "Field_1", "dataType": "String"}, {"name": "NodeCount", "dataType": "Integer"}, {"name": "WorkflowFullPath", "dataType": "String"}, {"name": "PositionY", "dataType": "Integer"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "NodeID", "dataType": "Integer"}]', 
        '[{"name": "Field_1", "dataType": "String"}, {"name": "NodeCount", "dataType": "Integer"}, {"name": "WorkflowFullPath", "dataType": "String"}, {"name": "PositionY", "dataType": "Integer"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "NodeID", "dataType": "Integer"}]', 
        '[{"name": "Field_1", "dataType": "String"}, {"name": "NodeCount", "dataType": "Integer"}, {"name": "WorkflowFullPath", "dataType": "String"}, {"name": "PositionY", "dataType": "Integer"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "NodeID", "dataType": "Integer"}]', 
        '[{"name": "Field_1", "dataType": "String"}, {"name": "NodeCount", "dataType": "Integer"}, {"name": "WorkflowFullPath", "dataType": "String"}, {"name": "PositionY", "dataType": "Integer"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "NodeID", "dataType": "Integer"}]', 
        '[{"name": "Field_1", "dataType": "String"}, {"name": "NodeCount", "dataType": "Integer"}, {"name": "WorkflowFullPath", "dataType": "String"}, {"name": "PositionY", "dataType": "Integer"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "NodeID", "dataType": "Integer"}]', 
        '[{"name": "Field_1", "dataType": "String"}, {"name": "NodeCount", "dataType": "Integer"}, {"name": "WorkflowFullPath", "dataType": "String"}, {"name": "PositionY", "dataType": "Integer"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "NodeID", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Sort_50 AS (

  SELECT * 
  
  FROM Union_49 AS in0
  
  ORDER BY WorkflowFullPath ASC, NodeID ASC, RecordID ASC

)

SELECT *

FROM Sort_50
