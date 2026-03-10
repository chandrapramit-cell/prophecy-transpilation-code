{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sort_46 AS (

  SELECT *
  
  FROM {{ ref('32_2022_main__Sort_46')}}

),

Summarize_47 AS (

  SELECT 
    SUM(CAST(LF AS NUMERIC)) AS Sum_LF,
    SUM(CAST(LS AS NUMERIC)) AS Sum_LS,
    SUM(CAST(UF AS NUMERIC)) AS Sum_UF,
    SUM(CAST(US AS NUMERIC)) AS Sum_US,
    GridX AS GridX,
    Team AS Team,
    mode AS mode,
    Match AS Match,
    GridY AS GridY
  
  FROM Sort_46 AS in0
  
  GROUP BY 
    GridX, Team, mode, Match, GridY

),

Formula_48_0 AS (

  SELECT 
    (CONCAT(Sum_UF, Sum_US)) AS TotalUpperShots,
    (CONCAT(Sum_LF, Sum_LS)) AS TotalLowerShots,
    *
  
  FROM Summarize_47 AS in0

),

Formula_48_1 AS (

  SELECT 
    CAST((CAST(Sum_US AS INT64) / TotalUpperShots) AS FLOAT64) AS UpperShotAccuracy,
    CAST((CAST(Sum_LS AS INT64) / TotalLowerShots) AS FLOAT64) AS LowerShotAccuracy,
    (TotalUpperShots + TotalLowerShots) AS TotalShots,
    *
  
  FROM Formula_48_0 AS in0

),

Formula_48_2 AS (

  SELECT 
    CAST(((CONCAT(Sum_US, Sum_LS)) / TotalShots) AS FLOAT64) AS TotalShotAccuracy,
    *
  
  FROM Formula_48_1 AS in0

),

Cleanse_51 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_48_2'], 
      [
        { "name": "Sum_UF", "dataType": "String" }, 
        { "name": "TotalUpperShots", "dataType": "Integer" }, 
        { "name": "Sum_US", "dataType": "String" }, 
        { "name": "GridX", "dataType": "Integer" }, 
        { "name": "TotalShotAccuracy", "dataType": "Double" }, 
        { "name": "LowerShotAccuracy", "dataType": "Double" }, 
        { "name": "UpperShotAccuracy", "dataType": "Double" }, 
        { "name": "Sum_LS", "dataType": "String" }, 
        { "name": "Team", "dataType": "Integer" }, 
        { "name": "mode", "dataType": "String" }, 
        { "name": "TotalShots", "dataType": "Integer" }, 
        { "name": "Match", "dataType": "Integer" }, 
        { "name": "GridY", "dataType": "Integer" }, 
        { "name": "Sum_LF", "dataType": "String" }, 
        { "name": "TotalLowerShots", "dataType": "Integer" }
      ], 
      'keepOriginal', 
      [
        'TotalUpperShots', 
        'TotalLowerShots', 
        'UpperShotAccuracy', 
        'LowerShotAccuracy', 
        'TotalShots', 
        'TotalShotAccuracy'
      ], 
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

)

SELECT *

FROM Cleanse_51
