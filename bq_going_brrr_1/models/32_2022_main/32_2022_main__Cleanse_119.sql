{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_117_2 AS (

  SELECT *
  
  FROM {{ ref('32_2022_main__Formula_117_2')}}

),

Formula_123_0 AS (

  SELECT 
    ((TotalSuccess / TotalShots) * 100) AS AccuracyPerMatch,
    *
  
  FROM Formula_117_2 AS in0

),

Cleanse_119 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_123_0'], 
      [
        { "name": "ClimbHighSuccess", "dataType": "Double" }, 
        { "name": "ScoutName", "dataType": "String" }, 
        { "name": "UploadTime", "dataType": "Integer" }, 
        { "name": "ClimbLowSuccess", "dataType": "Double" }, 
        { "name": "TeleLowerSuccess", "dataType": "Integer" }, 
        { "name": "variableTime", "dataType": "Integer" }, 
        { "name": "climbTotalTime", "dataType": "Double" }, 
        { "name": "InterfaceType", "dataType": "String" }, 
        { "name": "Points", "dataType": "Integer" }, 
        { "name": "matchIndex", "dataType": "Integer" }, 
        { "name": "UnderDefenseDuration", "dataType": "Integer" }, 
        { "name": "AutoUpperFailures", "dataType": "Integer" }, 
        { "name": "TotalSuccess", "dataType": "Integer" }, 
        { "name": "Event", "dataType": "String" }, 
        { "name": "climbMidTime", "dataType": "Double" }, 
        { "name": "ClimbMid", "dataType": "String" }, 
        { "name": "variableVersion", "dataType": "String" }, 
        { "name": "AllianceColor", "dataType": "Integer" }, 
        { "name": "ClimbTraversal", "dataType": "String" }, 
        { "name": "TeleLowerFailures", "dataType": "Integer" }, 
        { "name": "TotalLowerSuccess", "dataType": "Integer" }, 
        { "name": "TeleUpperSuccess", "dataType": "Integer" }, 
        { "name": "climbLowTime", "dataType": "Double" }, 
        { "name": "CommentWithMatch", "dataType": "String" }, 
        { "name": "climbHighTime", "dataType": "Double" }, 
        { "name": "ClimbMidSuccess", "dataType": "Double" }, 
        { "name": "TeleUpperFailures", "dataType": "Integer" }, 
        { "name": "AutoLowerFailures", "dataType": "Integer" }, 
        { "name": "PlayingDefenseDuration", "dataType": "Integer" }, 
        { "name": "Right_Match", "dataType": "Integer" }, 
        { "name": "ClimbLow", "dataType": "String" }, 
        { "name": "AutoLowerSuccess", "dataType": "Integer" }, 
        { "name": "Team", "dataType": "Integer" }, 
        { "name": "Taxi", "dataType": "Integer" }, 
        { "name": "Right_Team", "dataType": "Integer" }, 
        { "name": "ClimbLowPoints", "dataType": "Integer" }, 
        { "name": "ScoringData", "dataType": "String" }, 
        { "name": "DriverRating", "dataType": "Integer" }, 
        { "name": "TotalClimbPoints", "dataType": "Integer" }, 
        { "name": "StartPositionZone", "dataType": "Integer" }, 
        { "name": "StartPosition", "dataType": "String" }, 
        { "name": "ClimbLowAttempted", "dataType": "Double" }, 
        { "name": "ClimbHigh", "dataType": "String" }, 
        { "name": "AutoUpperSuccess", "dataType": "Integer" }, 
        { "name": "ClimbHighPoints", "dataType": "Integer" }, 
        { "name": "ClimbTraversalAttempted", "dataType": "Double" }, 
        { "name": "Penalties", "dataType": "Integer" }, 
        { "name": "ClimbTraversalSuccess", "dataType": "Double" }, 
        { "name": "Disabled", "dataType": "Integer" }, 
        { "name": "AccuracyPerMatch", "dataType": "Integer" }, 
        { "name": "TelePoints", "dataType": "Integer" }, 
        { "name": "TotalPoints", "dataType": "Integer" }, 
        { "name": "AutoPoints", "dataType": "Integer" }, 
        { "name": "DeviceName", "dataType": "String" }, 
        { "name": "TotalShots", "dataType": "Integer" }, 
        { "name": "ClimbTraversalPoints", "dataType": "Integer" }, 
        { "name": "Match", "dataType": "Integer" }, 
        { "name": "Comment", "dataType": "String" }, 
        { "name": "IntakeRating", "dataType": "Integer" }, 
        { "name": "UnderDefenseRating", "dataType": "Integer" }, 
        { "name": "ClimbMidAttempted", "dataType": "Double" }, 
        { "name": "climbTraversalTime", "dataType": "Double" }, 
        { "name": "DefenseRating", "dataType": "Decimal" }, 
        { "name": "ClimbHighAttempted", "dataType": "Double" }, 
        { "name": "TargetEvent", "dataType": "String" }, 
        { "name": "TotalUpperSuccess", "dataType": "Integer" }, 
        { "name": "ClimbMidPoints", "dataType": "Integer" }
      ], 
      'keepOriginal', 
      [
        'TargetEvent', 
        'Event', 
        'Team', 
        'Match', 
        'DeviceName', 
        'variableVersion', 
        'InterfaceType', 
        'variableTime', 
        'UploadTime', 
        'ScoutName', 
        'AllianceColor', 
        'StartPosition', 
        'StartPositionZone', 
        'Taxi', 
        'AutoUpperSuccess', 
        'AutoLowerSuccess', 
        'AutoUpperFailures', 
        'AutoLowerFailures', 
        'TeleUpperSuccess', 
        'TeleLowerSuccess', 
        'TeleUpperFailures', 
        'TeleLowerFailures', 
        'ScoringData', 
        'ClimbLow', 
        'ClimbMid', 
        'ClimbHigh', 
        'ClimbTraversal', 
        'Points', 
        'Penalties', 
        'Disabled', 
        'DriverRating', 
        'IntakeRating', 
        'DefenseRating', 
        'PlayingDefenseDuration', 
        'UnderDefenseRating', 
        'UnderDefenseDuration', 
        'Comment', 
        'matchIndex', 
        'TotalUpperSuccess', 
        'TotalLowerSuccess', 
        'TotalSuccess', 
        'TotalShots', 
        'CommentWithMatch', 
        'AutoPoints', 
        'TelePoints', 
        'Right_Team', 
        'Right_Match', 
        'ClimbLowAttempted', 
        'ClimbLowSuccess', 
        'ClimbMidAttempted', 
        'ClimbMidSuccess', 
        'ClimbHighAttempted', 
        'ClimbHighSuccess', 
        'ClimbTraversalAttempted', 
        'ClimbTraversalSuccess', 
        'climbLowTime', 
        'climbMidTime', 
        'climbHighTime', 
        'climbTraversalTime', 
        'climbTotalTime', 
        'ClimbLowPoints', 
        'ClimbMidPoints', 
        'ClimbHighPoints', 
        'ClimbTraversalPoints', 
        'TotalClimbPoints', 
        'TotalPoints', 
        'AccuracyPerMatch'
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

FROM Cleanse_119
