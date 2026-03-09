{{
  config({    
    "materialized": "table",
    "alias": "AutomaticWorkfl_53",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_10 AS (

  SELECT * 
  
  FROM {{ ref('seed_10')}}

),

TextInput_10_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_10 AS in0

),

AlteryxSelect_27 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_10_cast AS in0

),

RecordID_17 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM AlteryxSelect_27

),

DynamicSelect_55 AS (

  SELECT *
  
  FROM {{ ref('Runner_App_Generator__DynamicSelect_55')}}

),

AppendFields_46 AS (

  SELECT 
    in0.AYX_RecordID AS WFCount,
    in0.* EXCEPT (`AYX_RecordID`),
    in1.*
  
  FROM DynamicSelect_55 AS in0
  INNER JOIN RecordID_17 AS in1
     ON TRUE

),

Sort_47 AS (

  SELECT * 
  
  FROM AppendFields_46 AS in0
  
  ORDER BY WorkflowFullPath ASC, RecordID ASC

),

Formula_48_0 AS (

  SELECT 
    CAST((
      (
        coalesce(
          CAST((
            CASE
              WHEN (
                (
                  LENGTH(
                    CAST((REGEXP_EXTRACT((REGEXP_REPLACE(Field_1, '(?i)^.*(?:ToolID=")([0-9]+)(?:".*)$', '$1')), '^[0-9]+$', 0)) AS STRING))
                ) <= 0
              )
                THEN NULL
              ELSE (REGEXP_REPLACE(Field_1, '(?i)^.*(?:ToolID=")([0-9]+)(?:".*)$', '$1'))
            END
          ) AS FLOAT64), 
          CAST((
            REGEXP_EXTRACT(
              (
                CASE
                  WHEN (
                    (
                      LENGTH(
                        CAST((REGEXP_EXTRACT((REGEXP_REPLACE(Field_1, '(?i)^.*(?:ToolID=")([0-9]+)(?:".*)$', '$1')), '^[0-9]+$', 0)) AS STRING))
                    ) <= 0
                  )
                    THEN NULL
                  ELSE (REGEXP_REPLACE(Field_1, '(?i)^.*(?:ToolID=")([0-9]+)(?:".*)$', '$1'))
                END
              ), 
              '^[0-9]+', 
              0)
          ) AS INT64), 
          0)
      )
      + ((WFCount - 1) * 6)
    ) AS INT64) AS ToolID,
    *
  
  FROM Sort_47 AS in0

),

Formula_48_1 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        Field_1, 
        '(?i)(?<=ToolID=")([0-9]+)(?=")', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(ToolID AS FLOAT64))), ',', '__THS__')), '__THS__', '')))
    ) AS STRING) AS Field_1,
    * EXCEPT (`field_1`)
  
  FROM Formula_48_0 AS in0

),

Sort_51 AS (

  SELECT * 
  
  FROM Formula_48_1 AS in0
  
  ORDER BY WorkflowFullPath ASC, WFCount ASC, RecordID ASC

),

TextInput_8 AS (

  SELECT * 
  
  FROM {{ ref('seed_8')}}

),

TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_1')}}

),

TextInput_1_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_1 AS in0

),

AlteryxSelect_19 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_1_cast AS in0

),

TextInput_9 AS (

  SELECT * 
  
  FROM {{ ref('seed_9')}}

),

TextInput_8_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_8 AS in0

),

AlteryxSelect_26 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_8_cast AS in0

),

TextInput_9_cast AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_9 AS in0

),

Sort_50 AS (

  SELECT *
  
  FROM {{ ref('Runner_App_Generator__Sort_50')}}

),

AlteryxSelect_28 AS (

  SELECT CAST(Field_1 AS STRING) AS Field_1
  
  FROM TextInput_9_cast AS in0

),

Union_52 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_26', 'Sort_51', 'AlteryxSelect_19', 'Sort_50', 'AlteryxSelect_28'], 
      [
        '[{"name": "Field_1", "dataType": "String"}]', 
        '[{"name": "Field_1", "dataType": "String"}, {"name": "ToolID", "dataType": "Integer"}, {"name": "WorkflowFullPath", "dataType": "String"}, {"name": "WFCount", "dataType": "Integer"}, {"name": "RecordID", "dataType": "Integer"}]', 
        '[{"name": "Field_1", "dataType": "String"}]', 
        '[{"name": "Field_1", "dataType": "String"}, {"name": "NodeCount", "dataType": "Integer"}, {"name": "WorkflowFullPath", "dataType": "String"}, {"name": "PositionY", "dataType": "Integer"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "NodeID", "dataType": "Integer"}]', 
        '[{"name": "Field_1", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

DynamicSelect_54 AS (

  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

)

SELECT *

FROM DynamicSelect_54
