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

Summarize_5 AS (

  SELECT 
    AVG(AutoLowerSuccess) AS Avg_AutoLowerSuccess,
    AVG(AutoUpperSuccess) AS Avg_AutoUpperSuccess,
    AVG(TeleLowerSuccess) AS Avg_TeleLowerSuccess,
    AVG(TeleUpperSuccess) AS Avg_TeleUpperSuccess,
    AVG((
      CASE
        WHEN (AutoLowerSuccess = 0)
          THEN NULL
        ELSE AutoLowerSuccess
      END
    )) AS AvgNo0_AutoLowerSuccess,
    AVG((
      CASE
        WHEN (AutoUpperSuccess = 0)
          THEN NULL
        ELSE AutoUpperSuccess
      END
    )) AS AvgNo0_AutoUpperSuccess,
    AVG((
      CASE
        WHEN (TeleLowerSuccess = 0)
          THEN NULL
        ELSE TeleLowerSuccess
      END
    )) AS AvgNo0_TeleLowerSuccess,
    AVG((
      CASE
        WHEN (TeleUpperSuccess = 0)
          THEN NULL
        ELSE TeleUpperSuccess
      END
    )) AS AvgNo0_TeleUpperSuccess,
    STDDEV(AutoUpperSuccess) AS StdDev_AutoUpperSuccess,
    MAX(AutoUpperSuccess) AS Max_AutoUpperSuccess,
    STDDEV(AutoLowerSuccess) AS StdDev_AutoLowerSuccess,
    MAX(AutoLowerSuccess) AS Max_AutoLowerSuccess,
    STDDEV(TeleUpperSuccess) AS StdDev_TeleUpperSuccess,
    MAX(TeleUpperSuccess) AS Max_TeleUpperSuccess,
    STDDEV(TeleLowerSuccess) AS StdDev_TeleLowerSuccess,
    MAX(TeleLowerSuccess) AS Max_TeleLowerSuccess,
    AVG(TotalUpperSuccess) AS Avg_TotalUpperSuccess,
    STDDEV(TotalUpperSuccess) AS StdDev_TotalUpperSuccess,
    MAX(TotalUpperSuccess) AS Max_TotalUpperSuccess,
    AVG(TotalLowerSuccess) AS Avg_TotalLowerSuccess,
    STDDEV(TotalLowerSuccess) AS StdDev_TotalLowerSuccess,
    MAX(TotalLowerSuccess) AS Max_TotalLowerSuccess,
    AVG(TotalSuccess) AS Avg_TotalSuccess,
    STDDEV(TotalSuccess) AS StdDev_TotalSuccess,
    MAX(TotalSuccess) AS Max_TotalSuccess,
    AVG(TotalShots) AS Avg_TotalShots,
    STDDEV(TotalShots) AS StdDev_TotalShots,
    MAX(TotalShots) AS Max_TotalShots,
    AVG((
      CASE
        WHEN (DriverRating = 0)
          THEN NULL
        ELSE DriverRating
      END
    )) AS AvgNo0_DriverRating,
    AVG((
      CASE
        WHEN (IntakeRating = 0)
          THEN NULL
        ELSE IntakeRating
      END
    )) AS AvgNo0_IntakeRating,
    AVG((
      CASE
        WHEN (DefenseRating = 0)
          THEN NULL
        ELSE DefenseRating
      END
    )) AS AvgNo0_DefenseRating,
    count(CASE
      WHEN (isnull(Match) OR (CAST(Match AS STRING) = ''))
        THEN NULL
      ELSE 1
    END) AS `Count`,
    (ARRAY_TO_STRING((ARRAY_AGG(CommentWithMatch)), ' ')) AS Comment,
    SUM(ClimbLowAttempted) AS Sum_ClimbLowAttempted,
    SUM(ClimbLowSuccess) AS Sum_ClimbLowSuccess,
    SUM(ClimbMidAttempted) AS Sum_ClimbMidAttempted,
    SUM(ClimbMidSuccess) AS Sum_ClimbMidSuccess,
    SUM(ClimbHighAttempted) AS Sum_ClimbHighAttempted,
    SUM(ClimbHighSuccess) AS Sum_ClimbHighSuccess,
    SUM(ClimbTraversalAttempted) AS Sum_ClimbTraversalAttempted,
    SUM(ClimbTraversalSuccess) AS Sum_ClimbTraversalSuccess,
    AVG((
      CASE
        WHEN (climbLowTime = 0)
          THEN NULL
        ELSE climbLowTime
      END
    )) AS AvgNo0_climbLowTime,
    AVG((
      CASE
        WHEN (climbMidTime = 0)
          THEN NULL
        ELSE climbMidTime
      END
    )) AS AvgNo0_climbMidTime,
    AVG((
      CASE
        WHEN (climbHighTime = 0)
          THEN NULL
        ELSE climbHighTime
      END
    )) AS AvgNo0_climbHighTime,
    AVG((
      CASE
        WHEN (climbTraversalTime = 0)
          THEN NULL
        ELSE climbTraversalTime
      END
    )) AS AvgNo0_climbTraversalTime,
    AVG((
      CASE
        WHEN (climbTotalTime = 0)
          THEN NULL
        ELSE climbTotalTime
      END
    )) AS AvgNo0_climbTotalTime,
    AVG((
      CASE
        WHEN (UnderDefenseRating = 0)
          THEN NULL
        ELSE UnderDefenseRating
      END
    )) AS AvgNo0_UnderDefenseRating,
    SUM(TotalClimbPoints) AS Sum_TotalClimbPoints,
    SUM(AutoPoints) AS Sum_AutoPoints,
    SUM(TelePoints) AS Sum_TelePoints,
    SUM(TotalPoints) AS Sum_TotalPoints,
    AVG(TotalPoints) AS Avg_TotalPoints,
    SUM(Taxi) AS Sum_Taxi,
    STDDEV(Taxi) AS StdDev_Taxi,
    AVG(TotalClimbPoints) AS Avg_TotalClimbPoints,
    AVG(AutoPoints) AS Avg_AutoPoints,
    AVG(TelePoints) AS Avg_TelePoints,
    AVG((
      CASE
        WHEN (AutoPoints = 0)
          THEN NULL
        ELSE AutoPoints
      END
    )) AS AvgNo0_AutoPoints,
    AVG((
      CASE
        WHEN (TelePoints = 0)
          THEN NULL
        ELSE TelePoints
      END
    )) AS AvgNo0_TelePoints,
    AVG((
      CASE
        WHEN (TotalClimbPoints = 0)
          THEN NULL
        ELSE TotalClimbPoints
      END
    )) AS AvgNo0_TotalClimbPoints,
    Team AS Team
  
  FROM Formula_117_2 AS in0
  
  GROUP BY Team

),

Formula_68_0 AS (

  SELECT 
    (Sum_ClimbLowSuccess / Sum_ClimbLowAttempted) AS ClimbLowPercentage,
    (Sum_ClimbMidSuccess / Sum_ClimbMidAttempted) AS ClimbMidPercentage,
    (Sum_ClimbHighSuccess / Sum_ClimbHighAttempted) AS ClimbHighPercentage,
    (Sum_ClimbTraversalSuccess / Sum_ClimbTraversalAttempted) AS ClimbTraversalPercentage,
    *
  
  FROM Summarize_5 AS in0

),

Formula_68_1 AS (

  SELECT 
    (
      (((ClimbLowPercentage * 4) + (ClimbMidPercentage * 6)) + (ClimbHighPercentage * 10))
      + (ClimbTraversalPercentage * 15)
    ) AS Avg_ClimbPoints,
    *
  
  FROM Formula_68_0 AS in0

),

Cleanse_55 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_68_1'], 
      [
        { "name": "Avg_TotalShots", "dataType": "Double" }, 
        { "name": "StdDev_AutoUpperSuccess", "dataType": "Double" }, 
        { "name": "StdDev_TotalLowerSuccess", "dataType": "Double" }, 
        { "name": "StdDev_TotalShots", "dataType": "Double" }, 
        { "name": "Sum_ClimbHighAttempted", "dataType": "Double" }, 
        { "name": "AvgNo0_AutoUpperSuccess", "dataType": "Double" }, 
        { "name": "AvgNo0_climbMidTime", "dataType": "Double" }, 
        { "name": "Sum_Taxi", "dataType": "Integer" }, 
        { "name": "AvgNo0_TeleLowerSuccess", "dataType": "Double" }, 
        { "name": "Sum_ClimbLowSuccess", "dataType": "Double" }, 
        { "name": "Avg_TotalClimbPoints", "dataType": "Double" }, 
        { "name": "AvgNo0_TelePoints", "dataType": "Double" }, 
        { "name": "StdDev_AutoLowerSuccess", "dataType": "Double" }, 
        { "name": "Sum_ClimbTraversalAttempted", "dataType": "Double" }, 
        { "name": "Sum_TelePoints", "dataType": "Integer" }, 
        { "name": "AvgNo0_climbTotalTime", "dataType": "Double" }, 
        { "name": "StdDev_TotalUpperSuccess", "dataType": "Double" }, 
        { "name": "AvgNo0_IntakeRating", "dataType": "Double" }, 
        { "name": "Avg_TotalLowerSuccess", "dataType": "Double" }, 
        { "name": "Max_TotalShots", "dataType": "Integer" }, 
        { "name": "Sum_TotalClimbPoints", "dataType": "Integer" }, 
        { "name": "Sum_ClimbLowAttempted", "dataType": "Double" }, 
        { "name": "AvgNo0_UnderDefenseRating", "dataType": "Double" }, 
        { "name": "Sum_ClimbHighSuccess", "dataType": "Double" }, 
        { "name": "AvgNo0_DriverRating", "dataType": "Double" }, 
        { "name": "StdDev_TeleLowerSuccess", "dataType": "Double" }, 
        { "name": "Max_AutoLowerSuccess", "dataType": "Integer" }, 
        { "name": "Max_TeleLowerSuccess", "dataType": "Integer" }, 
        { "name": "Avg_ClimbPoints", "dataType": "Double" }, 
        { "name": "Sum_ClimbMidSuccess", "dataType": "Double" }, 
        { "name": "AvgNo0_TotalClimbPoints", "dataType": "Double" }, 
        { "name": "AvgNo0_climbHighTime", "dataType": "Double" }, 
        { "name": "ClimbMidPercentage", "dataType": "Double" }, 
        { "name": "Avg_TotalSuccess", "dataType": "Double" }, 
        { "name": "Sum_AutoPoints", "dataType": "Integer" }, 
        { "name": "Team", "dataType": "Integer" }, 
        { "name": "Avg_TeleUpperSuccess", "dataType": "Double" }, 
        { "name": "Max_TotalSuccess", "dataType": "Integer" }, 
        { "name": "Count", "dataType": "Double" }, 
        { "name": "Sum_ClimbMidAttempted", "dataType": "Double" }, 
        { "name": "Avg_TelePoints", "dataType": "Double" }, 
        { "name": "Avg_TotalPoints", "dataType": "Double" }, 
        { "name": "StdDev_Taxi", "dataType": "Double" }, 
        { "name": "ClimbLowPercentage", "dataType": "Double" }, 
        { "name": "Max_TeleUpperSuccess", "dataType": "Integer" }, 
        { "name": "AvgNo0_TeleUpperSuccess", "dataType": "Double" }, 
        { "name": "Avg_AutoUpperSuccess", "dataType": "Double" }, 
        { "name": "Avg_AutoPoints", "dataType": "Double" }, 
        { "name": "ClimbHighPercentage", "dataType": "Double" }, 
        { "name": "Max_TotalLowerSuccess", "dataType": "Integer" }, 
        { "name": "StdDev_TeleUpperSuccess", "dataType": "Double" }, 
        { "name": "StdDev_TotalSuccess", "dataType": "Double" }, 
        { "name": "Sum_ClimbTraversalSuccess", "dataType": "Double" }, 
        { "name": "AvgNo0_AutoPoints", "dataType": "Double" }, 
        { "name": "AvgNo0_climbLowTime", "dataType": "Double" }, 
        { "name": "Avg_AutoLowerSuccess", "dataType": "Double" }, 
        { "name": "AvgNo0_climbTraversalTime", "dataType": "Double" }, 
        { "name": "Comment", "dataType": "String" }, 
        { "name": "Avg_TotalUpperSuccess", "dataType": "Double" }, 
        { "name": "Sum_TotalPoints", "dataType": "Integer" }, 
        { "name": "ClimbTraversalPercentage", "dataType": "Double" }, 
        { "name": "Max_AutoUpperSuccess", "dataType": "Integer" }, 
        { "name": "AvgNo0_AutoLowerSuccess", "dataType": "Double" }, 
        { "name": "Avg_TeleLowerSuccess", "dataType": "Double" }, 
        { "name": "Max_TotalUpperSuccess", "dataType": "Integer" }, 
        { "name": "AvgNo0_DefenseRating", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'Avg_AutoLowerSuccess', 
        'Avg_AutoUpperSuccess', 
        'Avg_TeleLowerSuccess', 
        'Avg_TeleUpperSuccess', 
        'AvgNo0_AutoLowerSuccess', 
        'AvgNo0_AutoUpperSuccess', 
        'AvgNo0_TeleLowerSuccess', 
        'AvgNo0_TeleUpperSuccess', 
        'Team', 
        'StdDev_AutoUpperSuccess', 
        'Max_AutoUpperSuccess', 
        'StdDev_AutoLowerSuccess', 
        'Max_AutoLowerSuccess', 
        'StdDev_TeleUpperSuccess', 
        'Max_TeleUpperSuccess', 
        'StdDev_TeleLowerSuccess', 
        'Max_TeleLowerSuccess', 
        'Avg_TotalUpperSuccess', 
        'StdDev_TotalUpperSuccess', 
        'Max_TotalUpperSuccess', 
        'Avg_TotalLowerSuccess', 
        'StdDev_TotalLowerSuccess', 
        'Max_TotalLowerSuccess', 
        'Avg_TotalSuccess', 
        'StdDev_TotalSuccess', 
        'Max_TotalSuccess', 
        'Avg_TotalShots', 
        'StdDev_TotalShots', 
        'Max_TotalShots', 
        'AvgNo0_DriverRating', 
        'AvgNo0_IntakeRating', 
        'AvgNo0_DefenseRating', 
        'Count', 
        'Sum_ClimbLowAttempted', 
        'Sum_ClimbLowSuccess', 
        'Sum_ClimbMidAttempted', 
        'Sum_ClimbMidSuccess', 
        'Sum_ClimbHighAttempted', 
        'Sum_ClimbHighSuccess', 
        'Sum_ClimbTraversalAttempted', 
        'Sum_ClimbTraversalSuccess', 
        'AvgNo0_climbLowTime', 
        'AvgNo0_climbMidTime', 
        'AvgNo0_climbHighTime', 
        'AvgNo0_climbTraversalTime', 
        'AvgNo0_climbTotalTime', 
        'ClimbLowPercentage', 
        'ClimbMidPercentage', 
        'ClimbHighPercentage', 
        'ClimbTraversalPercentage', 
        'Avg_ClimbPoints'
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

FROM Cleanse_55
