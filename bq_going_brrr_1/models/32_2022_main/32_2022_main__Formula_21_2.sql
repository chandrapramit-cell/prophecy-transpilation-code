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

Formula_13_0 AS (

  SELECT 
    CAST((SUBSTRING(ClimbLow, 2, ((LENGTH(ClimbLow)) - 2))) AS STRING) AS FixedClimbLow,
    CAST((SUBSTRING(ClimbMid, 2, ((LENGTH(ClimbMid)) - 2))) AS STRING) AS FixedClimbMid,
    CAST((SUBSTRING(ClimbHigh, 2, ((LENGTH(ClimbHigh)) - 2))) AS STRING) AS FixedClimbHigh,
    CAST((SUBSTRING(ClimbTraversal, 2, ((LENGTH(ClimbTraversal)) - 2))) AS STRING) AS FixedClimbTraversal,
    *
  
  FROM MultiRowFormula_70_row_id_drop_0 AS in0

),

TextToColumns_14 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Formula_13_0'], 
      'FixedClimbLow', 
      ",", 
      'splitColumns', 
      6, 
      'leaveExtraCharLastCol', 
      'FixedClimbLow', 
      'FixedClimbLow', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_14_dropGem_0 AS (

  SELECT 
    FixedClimbLow_1_FixedClimbLow AS FixedClimbLow1,
    FixedClimbLow_2_FixedClimbLow AS FixedClimbLow2,
    FixedClimbLow_3_FixedClimbLow AS FixedClimbLow3,
    FixedClimbLow_4_FixedClimbLow AS FixedClimbLow4,
    FixedClimbLow_5_FixedClimbLow AS FixedClimbLow5,
    FixedClimbLow_6_FixedClimbLow AS FixedClimbLow6,
    * EXCEPT (`FixedClimbLow_1_FixedClimbLow`, 
    `FixedClimbLow_2_FixedClimbLow`, 
    `FixedClimbLow_3_FixedClimbLow`, 
    `FixedClimbLow_4_FixedClimbLow`, 
    `FixedClimbLow_5_FixedClimbLow`, 
    `FixedClimbLow_6_FixedClimbLow`)
  
  FROM TextToColumns_14 AS in0

),

TextToColumns_16 AS (

  {{
    prophecy_basics.TextToColumns(
      ['TextToColumns_14_dropGem_0'], 
      'FixedClimbMid', 
      ",", 
      'splitColumns', 
      6, 
      'leaveExtraCharLastCol', 
      'FixedClimbMid', 
      'FixedClimbMid', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_16_dropGem_0 AS (

  SELECT 
    FixedClimbMid_1_FixedClimbMid AS FixedClimbMid1,
    FixedClimbMid_2_FixedClimbMid AS FixedClimbMid2,
    FixedClimbMid_3_FixedClimbMid AS FixedClimbMid3,
    FixedClimbMid_4_FixedClimbMid AS FixedClimbMid4,
    FixedClimbMid_5_FixedClimbMid AS FixedClimbMid5,
    FixedClimbMid_6_FixedClimbMid AS FixedClimbMid6,
    * EXCEPT (`FixedClimbMid_1_FixedClimbMid`, 
    `FixedClimbMid_2_FixedClimbMid`, 
    `FixedClimbMid_3_FixedClimbMid`, 
    `FixedClimbMid_4_FixedClimbMid`, 
    `FixedClimbMid_5_FixedClimbMid`, 
    `FixedClimbMid_6_FixedClimbMid`)
  
  FROM TextToColumns_16 AS in0

),

TextToColumns_17 AS (

  {{
    prophecy_basics.TextToColumns(
      ['TextToColumns_16_dropGem_0'], 
      'FixedClimbHigh', 
      ",", 
      'splitColumns', 
      6, 
      'leaveExtraCharLastCol', 
      'FixedClimbHigh', 
      'FixedClimbHigh', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_17_dropGem_0 AS (

  SELECT 
    FixedClimbHigh_1_FixedClimbHigh AS FixedClimbHigh1,
    FixedClimbHigh_2_FixedClimbHigh AS FixedClimbHigh2,
    FixedClimbHigh_3_FixedClimbHigh AS FixedClimbHigh3,
    FixedClimbHigh_4_FixedClimbHigh AS FixedClimbHigh4,
    FixedClimbHigh_5_FixedClimbHigh AS FixedClimbHigh5,
    FixedClimbHigh_6_FixedClimbHigh AS FixedClimbHigh6,
    * EXCEPT (`FixedClimbHigh_1_FixedClimbHigh`, 
    `FixedClimbHigh_2_FixedClimbHigh`, 
    `FixedClimbHigh_3_FixedClimbHigh`, 
    `FixedClimbHigh_4_FixedClimbHigh`, 
    `FixedClimbHigh_5_FixedClimbHigh`, 
    `FixedClimbHigh_6_FixedClimbHigh`)
  
  FROM TextToColumns_17 AS in0

),

TextToColumns_18 AS (

  {{
    prophecy_basics.TextToColumns(
      ['TextToColumns_17_dropGem_0'], 
      'FixedClimbTraversal', 
      ",", 
      'splitColumns', 
      6, 
      'leaveExtraCharLastCol', 
      'FixedClimbTraversal', 
      'FixedClimbTraversal', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_18_dropGem_0 AS (

  SELECT 
    FixedClimbTraversal_1_FixedClimbTraversal AS FixedClimbTraversal1,
    FixedClimbTraversal_2_FixedClimbTraversal AS FixedClimbTraversal2,
    FixedClimbTraversal_3_FixedClimbTraversal AS FixedClimbTraversal3,
    FixedClimbTraversal_4_FixedClimbTraversal AS FixedClimbTraversal4,
    FixedClimbTraversal_5_FixedClimbTraversal AS FixedClimbTraversal5,
    FixedClimbTraversal_6_FixedClimbTraversal AS FixedClimbTraversal6,
    * EXCEPT (`FixedClimbTraversal_1_FixedClimbTraversal`, 
    `FixedClimbTraversal_2_FixedClimbTraversal`, 
    `FixedClimbTraversal_3_FixedClimbTraversal`, 
    `FixedClimbTraversal_4_FixedClimbTraversal`, 
    `FixedClimbTraversal_5_FixedClimbTraversal`, 
    `FixedClimbTraversal_6_FixedClimbTraversal`)
  
  FROM TextToColumns_18 AS in0

),

AlteryxSelect_19 AS (

  SELECT 
    CAST(FixedClimbLow1 AS FLOAT64) AS ClimbLowAttempted,
    CAST(FixedClimbLow2 AS FLOAT64) AS ClimbLowAttemptedTime,
    CAST(FixedClimbLow3 AS FLOAT64) AS ClimbLowSuccess,
    CAST(FixedClimbLow4 AS FLOAT64) AS ClimbLowSuccessTime,
    CAST(FixedClimbLow5 AS FLOAT64) AS ClimbLowFailure,
    CAST(FixedClimbLow6 AS FLOAT64) AS ClimbLowFailureTime,
    CAST(FixedClimbMid1 AS FLOAT64) AS ClimbMidAttempted,
    CAST(FixedClimbMid2 AS FLOAT64) AS ClimbMidAttemptedTime,
    CAST(FixedClimbMid3 AS FLOAT64) AS ClimbMidSuccess,
    CAST(FixedClimbMid4 AS FLOAT64) AS ClimbMidSuccessTime,
    CAST(FixedClimbMid5 AS FLOAT64) AS ClimbMidFailure,
    CAST(FixedClimbMid6 AS FLOAT64) AS ClimbMidFailureTime,
    CAST(FixedClimbHigh1 AS FLOAT64) AS ClimbHighAttempted,
    CAST(FixedClimbHigh2 AS FLOAT64) AS ClimbHighAttemptedTime,
    CAST(FixedClimbHigh3 AS FLOAT64) AS ClimbHighSuccess,
    CAST(FixedClimbHigh4 AS FLOAT64) AS ClimbHighSuccessTime,
    CAST(FixedClimbHigh5 AS FLOAT64) AS ClimbHighFailure,
    CAST(FixedClimbHigh6 AS FLOAT64) AS ClimbHighFailureTime,
    CAST(FixedClimbTraversal1 AS FLOAT64) AS ClimbTraversalAttempted,
    CAST(FixedClimbTraversal2 AS FLOAT64) AS ClimbTraversalAttemptedTime,
    CAST(FixedClimbTraversal3 AS FLOAT64) AS ClimbTraversalSuccess,
    CAST(FixedClimbTraversal4 AS FLOAT64) AS ClimbTraversalSuccessTime,
    CAST(FixedClimbTraversal5 AS FLOAT64) AS ClimbTraversalFailure,
    CAST(FixedClimbTraversal6 AS FLOAT64) AS ClimbTraversalFailureTime,
    * EXCEPT (`FixedClimbLow1`, 
    `FixedClimbLow2`, 
    `FixedClimbLow3`, 
    `FixedClimbLow4`, 
    `FixedClimbLow5`, 
    `FixedClimbLow6`, 
    `FixedClimbMid1`, 
    `FixedClimbMid2`, 
    `FixedClimbMid3`, 
    `FixedClimbMid4`, 
    `FixedClimbMid5`, 
    `FixedClimbMid6`, 
    `FixedClimbHigh1`, 
    `FixedClimbHigh2`, 
    `FixedClimbHigh3`, 
    `FixedClimbHigh4`, 
    `FixedClimbHigh5`, 
    `FixedClimbHigh6`, 
    `FixedClimbTraversal1`, 
    `FixedClimbTraversal2`, 
    `FixedClimbTraversal3`, 
    `FixedClimbTraversal4`, 
    `FixedClimbTraversal5`, 
    `FixedClimbTraversal6`)
  
  FROM TextToColumns_18_dropGem_0 AS in0

),

Cleanse_20 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_19'], 
      [
        { "name": "ClimbHighSuccess", "dataType": "Double" }, 
        { "name": "ScoutName", "dataType": "String" }, 
        { "name": "UploadTime", "dataType": "Integer" }, 
        { "name": "ClimbLowSuccess", "dataType": "Double" }, 
        { "name": "FixedClimbMid", "dataType": "String" }, 
        { "name": "TeleLowerSuccess", "dataType": "Integer" }, 
        { "name": "variableTime", "dataType": "Integer" }, 
        { "name": "InterfaceType", "dataType": "String" }, 
        { "name": "Points", "dataType": "Integer" }, 
        { "name": "ClimbHighAttemptedTime", "dataType": "Double" }, 
        { "name": "matchIndex", "dataType": "Integer" }, 
        { "name": "FixedClimbTraversal", "dataType": "String" }, 
        { "name": "UnderDefenseDuration", "dataType": "Integer" }, 
        { "name": "AutoUpperFailures", "dataType": "Integer" }, 
        { "name": "Event", "dataType": "String" }, 
        { "name": "ClimbMid", "dataType": "String" }, 
        { "name": "variableVersion", "dataType": "String" }, 
        { "name": "AllianceColor", "dataType": "Integer" }, 
        { "name": "ClimbTraversal", "dataType": "String" }, 
        { "name": "TeleLowerFailures", "dataType": "Integer" }, 
        { "name": "ClimbMidFailureTime", "dataType": "Double" }, 
        { "name": "TeleUpperSuccess", "dataType": "Integer" }, 
        { "name": "ClimbMidAttemptedTime", "dataType": "Double" }, 
        { "name": "ClimbMidSuccess", "dataType": "Double" }, 
        { "name": "TeleUpperFailures", "dataType": "Integer" }, 
        { "name": "ClimbTraversalSuccessTime", "dataType": "Double" }, 
        { "name": "ClimbLowSuccessTime", "dataType": "Double" }, 
        { "name": "AutoLowerFailures", "dataType": "Integer" }, 
        { "name": "PlayingDefenseDuration", "dataType": "Integer" }, 
        { "name": "ClimbLow", "dataType": "String" }, 
        { "name": "AutoLowerSuccess", "dataType": "Integer" }, 
        { "name": "ClimbHighFailure", "dataType": "Double" }, 
        { "name": "Team", "dataType": "Integer" }, 
        { "name": "Taxi", "dataType": "Integer" }, 
        { "name": "ClimbTraversalFailureTime", "dataType": "Double" }, 
        { "name": "ClimbMidSuccessTime", "dataType": "Double" }, 
        { "name": "ClimbHighSuccessTime", "dataType": "Double" }, 
        { "name": "ClimbTraversalFailure", "dataType": "Double" }, 
        { "name": "ScoringData", "dataType": "String" }, 
        { "name": "DriverRating", "dataType": "Integer" }, 
        { "name": "StartPositionZone", "dataType": "Integer" }, 
        { "name": "StartPosition", "dataType": "String" }, 
        { "name": "ClimbLowAttempted", "dataType": "Double" }, 
        { "name": "ClimbHigh", "dataType": "String" }, 
        { "name": "AutoUpperSuccess", "dataType": "Integer" }, 
        { "name": "ClimbTraversalAttemptedTime", "dataType": "Double" }, 
        { "name": "ClimbLowAttemptedTime", "dataType": "Double" }, 
        { "name": "ClimbTraversalAttempted", "dataType": "Double" }, 
        { "name": "ClimbLowFailure", "dataType": "Double" }, 
        { "name": "FixedClimbHigh", "dataType": "String" }, 
        { "name": "Penalties", "dataType": "Integer" }, 
        { "name": "ClimbTraversalSuccess", "dataType": "Double" }, 
        { "name": "Disabled", "dataType": "Integer" }, 
        { "name": "ClimbMidFailure", "dataType": "Double" }, 
        { "name": "ClimbLowFailureTime", "dataType": "Double" }, 
        { "name": "DeviceName", "dataType": "String" }, 
        { "name": "FixedClimbLow", "dataType": "String" }, 
        { "name": "Match", "dataType": "Integer" }, 
        { "name": "Comment", "dataType": "String" }, 
        { "name": "IntakeRating", "dataType": "Integer" }, 
        { "name": "UnderDefenseRating", "dataType": "Integer" }, 
        { "name": "ClimbMidAttempted", "dataType": "Double" }, 
        { "name": "DefenseRating", "dataType": "Decimal" }, 
        { "name": "ClimbHighAttempted", "dataType": "Double" }, 
        { "name": "TargetEvent", "dataType": "String" }, 
        { "name": "ClimbHighFailureTime", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'ClimbLowAttempted', 
        'ClimbLowAttemptedTime', 
        'ClimbLowSuccess', 
        'ClimbLowSuccessTime', 
        'ClimbLowFailure', 
        'ClimbLowFailureTime', 
        'ClimbMidAttempted', 
        'ClimbMidAttemptedTime', 
        'ClimbMidSuccess', 
        'ClimbMidSuccessTime', 
        'ClimbMidFailure', 
        'ClimbMidFailureTime', 
        'ClimbHighAttempted', 
        'ClimbHighAttemptedTime', 
        'ClimbHighSuccess', 
        'ClimbHighSuccessTime', 
        'ClimbHighFailure', 
        'ClimbHighFailureTime', 
        'ClimbTraversalAttempted', 
        'ClimbTraversalAttemptedTime', 
        'ClimbTraversalSuccess', 
        'ClimbTraversalSuccessTime', 
        'ClimbTraversalFailure', 
        'ClimbTraversalFailureTime'
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

),

Formula_21_0 AS (

  SELECT 
    (
      CASE
        WHEN (ClimbLowSuccess = 1)
          THEN (ClimbLowSuccessTime - ClimbLowAttemptedTime)
        ELSE 0
      END
    ) AS climbLowTime,
    (
      CASE
        WHEN (ClimbMidSuccess = 1)
          THEN (ClimbMidSuccessTime - ClimbMidAttemptedTime)
        ELSE 0
      END
    ) AS climbMidTime,
    (
      CASE
        WHEN (ClimbHighSuccess = 1)
          THEN (ClimbHighSuccessTime - ClimbHighAttemptedTime)
        ELSE 0
      END
    ) AS climbHighTime,
    (
      CASE
        WHEN (ClimbTraversalSuccess = 1)
          THEN (ClimbTraversalSuccessTime - ClimbTraversalAttemptedTime)
        ELSE 0
      END
    ) AS climbTraversalTime,
    *
  
  FROM Cleanse_20 AS in0

),

Formula_21_1 AS (

  SELECT 
    (((climbLowTime + climbMidTime) + climbHighTime) + climbTraversalTime) AS climbTotalTime,
    CAST(concat('{\'ScoringLocations\':', ScoringData, '}') AS STRING) AS FixedScoringData,
    *
  
  FROM Formula_21_0 AS in0

),

Formula_21_2 AS (

  SELECT 
    CAST(regexp_replace(FixedScoringData, '\'', '"') AS STRING) AS FixedScoringData,
    * EXCEPT (`fixedscoringdata`)
  
  FROM Formula_21_1 AS in0

)

SELECT *

FROM Formula_21_2
