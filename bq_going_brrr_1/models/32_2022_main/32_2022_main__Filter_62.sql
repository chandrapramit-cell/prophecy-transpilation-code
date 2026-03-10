{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH MultiRowFormula_70_row_id_drop_0 AS (

  SELECT *
  
  FROM {{ ref('32_2022_main__MultiRowFormula_70_row_id_drop_0')}}

),

AlteryxSelect_56 AS (

  SELECT 
    TargetEvent AS TargetEvent,
    Event AS Event,
    Team AS Team,
    StartPosition AS StartPosition
  
  FROM MultiRowFormula_70_row_id_drop_0 AS in0

),

TextToColumns_57 AS (

  {{
    prophecy_basics.TextToColumns(
      ['AlteryxSelect_56'], 
      'StartPosition', 
      ",", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'StartPosition', 
      'StartPosition', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_57_dropGem_0 AS (

  SELECT 
    StartPosition_1_StartPosition AS StartPosition1,
    StartPosition_2_StartPosition AS StartPosition2,
    * EXCEPT (`StartPosition_1_StartPosition`, `StartPosition_2_StartPosition`)
  
  FROM TextToColumns_57 AS in0

),

AlteryxSelect_60 AS (

  SELECT 
    CAST(StartPosition1 AS INT64) AS x,
    CAST(StartPosition2 AS INT64) AS y,
    * EXCEPT (`StartPosition1`, `StartPosition2`)
  
  FROM TextToColumns_57_dropGem_0 AS in0

),

Cleanse_61 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_60'], 
      [
        { "name": "x", "dataType": "Integer" }, 
        { "name": "Event", "dataType": "String" }, 
        { "name": "y", "dataType": "Integer" }, 
        { "name": "Team", "dataType": "Integer" }, 
        { "name": "StartPosition", "dataType": "String" }, 
        { "name": "TargetEvent", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['x', 'y'], 
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

Formula_59_0 AS (

  SELECT 
    FLOOR((x / 100)) AS GridX,
    FLOOR((y / 100)) AS GridY,
    *
  
  FROM Cleanse_61 AS in0

),

Filter_62 AS (

  SELECT * 
  
  FROM Formula_59_0 AS in0
  
  WHERE (((GridX <> 0) OR (GridX IS NULL)) AND ((GridY <> 0) OR (GridY IS NULL)))

)

SELECT *

FROM Filter_62
