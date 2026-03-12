{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH JSONParse_24 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('32_2022_main', 'JSONParse_24') }}

),

AlteryxSelect_26 AS (

  SELECT 
    Team AS Team,
    Match AS Match,
    AllianceColor AS AllianceColor,
    JSON_Name AS JSON_Name,
    JSON_ValueString AS JSON_ValueString
  
  FROM JSONParse_24 AS in0

),

Sort_27 AS (

  SELECT * 
  
  FROM AlteryxSelect_26 AS in0
  
  ORDER BY Team ASC, Match ASC, JSON_Name ASC

),

Filter_29 AS (

  SELECT * 
  
  FROM Sort_27 AS in0
  
  WHERE (JSON_Name IS NOT NULL)

),

TextToColumns_31 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Filter_29'], 
      'JSON_Name', 
      "\\\.", 
      'splitColumns', 
      3, 
      'leaveExtraCharLastCol', 
      'JSON_Name', 
      'JSON_Name', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_31_dropGem_0 AS (

  SELECT 
    JSON_Name_1_JSON_Name AS JSON_Name1,
    JSON_Name_2_JSON_Name AS JSON_Name2,
    JSON_Name_3_JSON_Name AS JSON_Name3,
    * EXCEPT (`JSON_Name_1_JSON_Name`, `JSON_Name_2_JSON_Name`, `JSON_Name_3_JSON_Name`)
  
  FROM TextToColumns_31 AS in0

),

Sort_35 AS (

  SELECT * 
  
  FROM TextToColumns_31_dropGem_0 AS in0
  
  ORDER BY Team ASC, Match ASC, JSON_Name2 ASC, JSON_Name3 ASC

),

MultiRowFormula_38_row_id_0 AS (

  SELECT 
    (ROW_NUMBER() OVER ()) AS prophecy_row_id,
    *
  
  FROM Sort_35 AS in0

),

MultiRowFormula_38 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_38_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_38 AS in0

),

CrossTab_34 AS (

  SELECT *
  
  FROM (
    SELECT 
      Team,
      Match,
      JSON_Name2,
      RecordID,
      JSON_Name3,
      JSON_VALUESTRING
    
    FROM MultiRowFormula_38_row_id_drop_0 AS in0
  )
  PIVOT (
    STRING_AGG(JSON_VALUESTRING, ', ') AS Concat
    FOR JSON_Name3
    IN (
      'x', 'y', 'US', 'UF', 'ShootingPositionZone', 'LS', 'mode', 'LF', 'time'
    )
  )

),

CrossTab_34_rename AS (

  SELECT 
    Concat_x AS x,
    Concat_y AS y,
    Concat_US AS US,
    Concat_UF AS UF,
    Concat_ShootingPositionZone AS ShootingPositionZone,
    Concat_LS AS LS,
    Concat_mode AS mode,
    Concat_LF AS LF,
    Concat_time AS variabletime,
    * EXCEPT (`Concat_x`, 
    `Concat_y`, 
    `Concat_US`, 
    `Concat_UF`, 
    `Concat_ShootingPositionZone`, 
    `Concat_LS`, 
    `Concat_mode`, 
    `Concat_LF`, 
    `Concat_time`)
  
  FROM CrossTab_34 AS in0

),

Formula_39_0 AS (

  SELECT 
    FLOOR((CAST(x AS INT64) / 100)) AS GridX,
    FLOOR((CAST(y AS INT64) / 100)) AS GridY,
    (
      CASE
        WHEN (CAST(ShootingPositionZone AS INT64) = 1)
          THEN 1015
        WHEN (CAST(ShootingPositionZone AS INT64) = 2)
          THEN 800
        WHEN (CAST(ShootingPositionZone AS INT64) = 3)
          THEN 1120
        WHEN (CAST(ShootingPositionZone AS INT64) = 4)
          THEN 1050
        WHEN (CAST(ShootingPositionZone AS INT64) = 5)
          THEN 460
        WHEN (CAST(ShootingPositionZone AS INT64) = 6)
          THEN 415
        WHEN (CAST(ShootingPositionZone AS INT64) = 7)
          THEN 1260
        WHEN (CAST(ShootingPositionZone AS INT64) = 8)
          THEN 1350
        WHEN (CAST(ShootingPositionZone AS INT64) = 9)
          THEN 1360
        WHEN (CAST(ShootingPositionZone AS INT64) = 10)
          THEN 1600
        WHEN (CAST(ShootingPositionZone AS INT64) = 11)
          THEN 1900
        ELSE 2000
      END
    ) AS ZoneX,
    (
      CASE
        WHEN (CAST(ShootingPositionZone AS INT64) = 1)
          THEN 530
        WHEN (CAST(ShootingPositionZone AS INT64) = 2)
          THEN 450
        WHEN (CAST(ShootingPositionZone AS INT64) = 3)
          THEN 775
        WHEN (CAST(ShootingPositionZone AS INT64) = 4)
          THEN 1000
        WHEN (CAST(ShootingPositionZone AS INT64) = 5)
          THEN 430
        WHEN (CAST(ShootingPositionZone AS INT64) = 6)
          THEN 775
        WHEN (CAST(ShootingPositionZone AS INT64) = 7)
          THEN 415
        WHEN (CAST(ShootingPositionZone AS INT64) = 8)
          THEN 200
        WHEN (CAST(ShootingPositionZone AS INT64) = 9)
          THEN 660
        WHEN (CAST(ShootingPositionZone AS INT64) = 10)
          THEN 750
        WHEN (CAST(ShootingPositionZone AS INT64) = 11)
          THEN 775
        ELSE 410
      END
    ) AS ZoneY,
    *
  
  FROM CrossTab_34_rename AS in0

)

SELECT *

FROM Formula_39_0
