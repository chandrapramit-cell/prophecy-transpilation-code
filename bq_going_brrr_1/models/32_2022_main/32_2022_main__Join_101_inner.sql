{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Cleanse_55 AS (

  SELECT *
  
  FROM {{ ref('32_2022_main__Cleanse_55')}}

),

Macro_71 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_75 AS (

  SELECT 
    Percentile AS `AvgAutoLowerSuccessPercentile`,
    *
  
  FROM Macro_71

),

Macro_74 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_76 AS (

  SELECT 
    Percentile AS `AvgAutoUpperSuccessPercentile`,
    *
  
  FROM Macro_74

),

Macro_77 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_78 AS (

  SELECT 
    Percentile AS `AvgTeleLowerSuccessPercentile`,
    *
  
  FROM Macro_77

),

Macro_79 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_80 AS (

  SELECT 
    Percentile AS `AvgTeleUpperSuccessPercentile`,
    *
  
  FROM Macro_79

),

Macro_81 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_82 AS (

  SELECT 
    Percentile AS `MaxAutoLowerSuccessPercentile`,
    *
  
  FROM Macro_81

),

Macro_83 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_84 AS (

  SELECT 
    Percentile AS `MaxAutoUpperSuccessPercentile`,
    *
  
  FROM Macro_83

),

Macro_85 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_86 AS (

  SELECT 
    Percentile AS `MaxTeleLowerSuccessPercentile`,
    *
  
  FROM Macro_85

),

Macro_87 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_88 AS (

  SELECT 
    Percentile AS `MaxTeleUpperSuccessPercentile`,
    *
  
  FROM Macro_87

),

Macro_89 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_90 AS (

  SELECT 
    Percentile AS `AvgTotalSuccessPercentile`,
    *
  
  FROM Macro_89

),

Macro_91 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_92 AS (

  SELECT 
    Percentile AS `MaxTotalSuccessPercentile`,
    *
  
  FROM Macro_91

),

Macro_93 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_94 AS (

  SELECT 
    Percentile AS `AvgDefenseRatingPercentile`,
    *
  
  FROM Macro_93

),

Macro_105 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_106 AS (

  SELECT 
    Percentile AS `Avg_TotalShotsPercentile`,
    *
  
  FROM Macro_105

),

Macro_107 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_108 AS (

  SELECT 
    Percentile AS `ClimbHighPercentagePercentile`,
    *
  
  FROM Macro_107

),

Macro_109 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_110 AS (

  SELECT 
    Percentile AS `ClimbLowPercentagePercentile`,
    *
  
  FROM Macro_109

),

Macro_111 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_112 AS (

  SELECT 
    Percentile AS `ClimbMidPercentagePercentile`,
    *
  
  FROM Macro_111

),

Macro_121 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_122 AS (

  SELECT 
    Percentile AS `Sum_Taxi`,
    *
  
  FROM Macro_121

),

Macro_113 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_114 AS (

  SELECT 
    Percentile AS `ClimbTraversalPercentagePercentile`,
    *
  
  FROM Macro_113

),

Macro_115 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file Users/Admin/Downloads/Percentile/Percentile.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_116 AS (

  SELECT 
    Percentile AS Avg_ClimbPointsPercentile,
    * EXCEPT (`Percentile`)
  
  FROM Macro_115 AS in0

),

AlteryxSelect_98 AS (

  SELECT 
    AvgAutoLowerSuccessPercentile AS Avg_AutoLowerSuccess,
    AvgAutoUpperSuccessPercentile AS Avg_AutoUpperSuccess,
    AvgTeleLowerSuccessPercentile AS Avg_TeleLowerSuccess,
    AvgTeleUpperSuccessPercentile AS Avg_TeleUpperSuccess,
    MaxAutoLowerSuccessPercentile AS Max_AutoLowerSuccess,
    MaxAutoUpperSuccessPercentile AS Max_AutoUpperSuccess,
    MaxTeleLowerSuccessPercentile AS Max_TeleLowerSuccess,
    MaxTeleUpperSuccessPercentile AS Max_TeleUpperSuccess,
    AvgTotalSuccessPercentile AS Avg_TotalSuccess,
    MaxTotalSuccessPercentile AS Max_TotalSuccess,
    AvgDefenseRatingPercentile AS Avg_DefenseRating,
    Avg_TotalShotsPercentile AS Avg_TotalShots,
    ClimbHighPercentagePercentile AS ClimbHighPercentage,
    ClimbLowPercentagePercentile AS ClimbLowPercentage,
    ClimbMidPercentagePercentile AS ClimbMidPercentage,
    ClimbTraversalPercentagePercentile AS ClimbTraversalPercentage,
    Avg_ClimbPointsPercentile AS Avg_ClimbPoints,
    * EXCEPT (`Avg_AutoLowerSuccess`, 
    `Avg_AutoUpperSuccess`, 
    `Avg_TeleLowerSuccess`, 
    `Avg_TeleUpperSuccess`, 
    `StdDev_AutoUpperSuccess`, 
    `Max_AutoUpperSuccess`, 
    `StdDev_AutoLowerSuccess`, 
    `Max_AutoLowerSuccess`, 
    `StdDev_TeleUpperSuccess`, 
    `Max_TeleUpperSuccess`, 
    `StdDev_TeleLowerSuccess`, 
    `Max_TeleLowerSuccess`, 
    `Avg_TotalUpperSuccess`, 
    `StdDev_TotalUpperSuccess`, 
    `Max_TotalUpperSuccess`, 
    `Avg_TotalSuccess`, 
    `StdDev_TotalSuccess`, 
    `Max_TotalSuccess`, 
    `Avg_TotalShots`, 
    `StdDev_TotalShots`, 
    `ClimbLowPercentage`, 
    `ClimbMidPercentage`, 
    `ClimbHighPercentage`, 
    `ClimbTraversalPercentage`, 
    `Avg_ClimbPoints`, 
    `AvgAutoLowerSuccessPercentile`, 
    `AvgAutoUpperSuccessPercentile`, 
    `AvgTeleLowerSuccessPercentile`, 
    `AvgTeleUpperSuccessPercentile`, 
    `MaxAutoLowerSuccessPercentile`, 
    `MaxAutoUpperSuccessPercentile`, 
    `MaxTeleLowerSuccessPercentile`, 
    `MaxTeleUpperSuccessPercentile`, 
    `AvgTotalSuccessPercentile`, 
    `MaxTotalSuccessPercentile`, 
    `AvgDefenseRatingPercentile`, 
    `Avg_TotalShotsPercentile`, 
    `ClimbHighPercentagePercentile`, 
    `ClimbLowPercentagePercentile`, 
    `ClimbMidPercentagePercentile`, 
    `ClimbTraversalPercentagePercentile`, 
    `Avg_ClimbPointsPercentile`)
  
  FROM AlteryxSelect_116 AS in0

),

Formula_99_0 AS (

  SELECT 
    0 AS StdDev_Avg_AutoUpperSuccess,
    0 AS StdDev_AutoLowerSuccess,
    0 AS StdDev_TeleUpperSuccess,
    0 AS StdDev_TeleLowerSuccess,
    0 AS StdDev_TotalSuccess,
    0 AS StdDev_Max_TotalSuccess,
    0 AS StdDev_Max_AutoLowerSuccess,
    0 AS StdDev_Max_AutoUpperSuccess,
    0 AS StdDev_Max_TeleLowerSuccess,
    0 AS StdDev_Max_TeleUpperSuccess,
    0 AS StdDev_AvgNo0_DefenseRating,
    *
  
  FROM AlteryxSelect_98 AS in0

),

Transpose_100 AS (

  {{
    prophecy_basics.Transpose(
      ['Formula_99_0'], 
      ['Team'], 
      [
        'Sum_TotalClimbPoints', 
        'Sum_AutoPoints', 
        'Sum_TelePoints', 
        'Sum_TotalPoints', 
        'Avg_TotalPoints', 
        'Sum_Taxi', 
        'StdDev_Taxi', 
        'Avg_ClimbPoints2', 
        'Avg_AutoLowerSuccess', 
        'Avg_AutoUpperSuccess', 
        'Avg_TeleLowerSuccess', 
        'Avg_TeleUpperSuccess', 
        'Max_AutoLowerSuccess', 
        'Max_AutoUpperSuccess', 
        'Max_TeleLowerSuccess', 
        'Max_TeleUpperSuccess', 
        'Avg_TotalSuccess', 
        'Max_TotalSuccess', 
        'Avg_DefenseRating', 
        'Avg_TotalShots', 
        'ClimbHighPercentage', 
        'ClimbLowPercentage', 
        'ClimbMidPercentage', 
        'TaxiPercentile', 
        'ClimbTraversalPercentage', 
        'Avg_ClimbPoints', 
        'StdDev_Avg_AutoUpperSuccess', 
        'StdDev_AutoLowerSuccess', 
        'StdDev_TeleUpperSuccess', 
        'StdDev_TeleLowerSuccess', 
        'StdDev_TotalSuccess', 
        'StdDev_Max_TotalSuccess', 
        'StdDev_Max_AutoLowerSuccess', 
        'StdDev_Max_AutoUpperSuccess', 
        'StdDev_Max_TeleLowerSuccess', 
        'StdDev_Max_TeleUpperSuccess', 
        'StdDev_AvgNo0_DefenseRating', 
        'AverageClimbPoints'
      ], 
      'Name', 
      'Value', 
      [
        'Avg_TotalShots', 
        'Avg_DefenseRating', 
        'StdDev_Max_TeleUpperSuccess', 
        'Sum_Taxi', 
        'StdDev_Max_TeleLowerSuccess', 
        'StdDev_AvgNo0_DefenseRating', 
        'StdDev_Max_AutoUpperSuccess', 
        'StdDev_Max_AutoLowerSuccess', 
        'StdDev_AutoLowerSuccess', 
        'Sum_TelePoints', 
        'Sum_TotalClimbPoints', 
        'StdDev_TeleLowerSuccess', 
        'Max_AutoLowerSuccess', 
        'Max_TeleLowerSuccess', 
        'Avg_ClimbPoints', 
        'StdDev_Avg_AutoUpperSuccess', 
        'ClimbMidPercentage', 
        'Avg_TotalSuccess', 
        'Sum_AutoPoints', 
        'Team', 
        'Avg_TeleUpperSuccess', 
        'Max_TotalSuccess', 
        'Avg_ClimbPoints2', 
        'Avg_TotalPoints', 
        'StdDev_Taxi', 
        'ClimbLowPercentage', 
        'Max_TeleUpperSuccess', 
        'TaxiPercentile', 
        'Avg_AutoUpperSuccess', 
        'StdDev_Max_TotalSuccess', 
        'ClimbHighPercentage', 
        'AverageClimbPoints', 
        'StdDev_TeleUpperSuccess', 
        'StdDev_TotalSuccess', 
        'Avg_AutoLowerSuccess', 
        'Sum_TotalPoints', 
        'ClimbTraversalPercentage', 
        'Max_AutoUpperSuccess', 
        'Avg_TeleLowerSuccess'
      ], 
      true
    )
  }}

),

AlteryxSelect_95 AS (

  SELECT 
    Avg_AutoLowerSuccess AS Avg_AutoLowerSuccess,
    Avg_AutoUpperSuccess AS Avg_AutoUpperSuccess,
    Avg_TeleLowerSuccess AS Avg_TeleLowerSuccess,
    Avg_TeleUpperSuccess AS Avg_TeleUpperSuccess,
    Team AS Team,
    StdDev_AutoUpperSuccess AS StdDev_AutoUpperSuccess,
    Max_AutoUpperSuccess AS Max_AutoUpperSuccess,
    StdDev_AutoLowerSuccess AS StdDev_AutoLowerSuccess,
    Max_AutoLowerSuccess AS Max_AutoLowerSuccess,
    StdDev_TeleUpperSuccess AS StdDev_TeleUpperSuccess,
    Max_TeleUpperSuccess AS Max_TeleUpperSuccess,
    StdDev_TeleLowerSuccess AS StdDev_TeleLowerSuccess,
    Max_TeleLowerSuccess AS Max_TeleLowerSuccess,
    Avg_TotalUpperSuccess AS Avg_TotalUpperSuccess,
    StdDev_TotalUpperSuccess AS StdDev_TotalUpperSuccess,
    Max_TotalUpperSuccess AS Max_TotalUpperSuccess,
    Avg_TotalSuccess AS Avg_TotalSuccess,
    StdDev_TotalSuccess AS StdDev_TotalSuccess,
    Max_TotalSuccess AS Max_TotalSuccess,
    Avg_TotalShots AS Avg_TotalShots,
    StdDev_TotalShots AS StdDev_TotalShots,
    ClimbLowPercentage AS ClimbLowPercentage,
    ClimbMidPercentage AS ClimbMidPercentage,
    ClimbHighPercentage AS ClimbHighPercentage,
    ClimbTraversalPercentage AS ClimbTraversalPercentage,
    Avg_ClimbPoints AS Avg_ClimbPoints,
    Sum_Taxi AS Sum_Taxi,
    AverageClimbPoints AS AverageClimbPoints
  
  FROM AlteryxSelect_116 AS in0

),

Transpose_96 AS (

  {{
    prophecy_basics.Transpose(
      ['AlteryxSelect_95'], 
      ['Team'], 
      [
        'Avg_AutoLowerSuccess', 
        'Avg_AutoUpperSuccess', 
        'Avg_TeleLowerSuccess', 
        'Avg_TeleUpperSuccess', 
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
        'Avg_TotalSuccess', 
        'StdDev_TotalSuccess', 
        'Max_TotalSuccess', 
        'Avg_TotalShots', 
        'StdDev_TotalShots', 
        'ClimbLowPercentage', 
        'ClimbMidPercentage', 
        'ClimbHighPercentage', 
        'ClimbTraversalPercentage', 
        'Avg_ClimbPoints', 
        'Sum_Taxi', 
        'AverageClimbPoints'
      ], 
      'Name', 
      'Value', 
      [
        'Avg_TotalShots', 
        'StdDev_AutoUpperSuccess', 
        'StdDev_TotalShots', 
        'Sum_Taxi', 
        'StdDev_AutoLowerSuccess', 
        'StdDev_TotalUpperSuccess', 
        'StdDev_TeleLowerSuccess', 
        'Max_AutoLowerSuccess', 
        'Max_TeleLowerSuccess', 
        'Avg_ClimbPoints', 
        'ClimbMidPercentage', 
        'Avg_TotalSuccess', 
        'Team', 
        'Avg_TeleUpperSuccess', 
        'Max_TotalSuccess', 
        'ClimbLowPercentage', 
        'Max_TeleUpperSuccess', 
        'Avg_AutoUpperSuccess', 
        'ClimbHighPercentage', 
        'AverageClimbPoints', 
        'StdDev_TeleUpperSuccess', 
        'StdDev_TotalSuccess', 
        'Avg_AutoLowerSuccess', 
        'Avg_TotalUpperSuccess', 
        'ClimbTraversalPercentage', 
        'Max_AutoUpperSuccess', 
        'Avg_TeleLowerSuccess', 
        'Max_TotalUpperSuccess'
      ], 
      true
    )
  }}

),

Join_101_inner AS (

  SELECT 
    in0.Name AS Name,
    in1.VALUE AS Percentile,
    in0.* EXCEPT (`Name`)
  
  FROM Transpose_96 AS in0
  INNER JOIN Transpose_100 AS in1
     ON ((in0.Team = in1.Team) AND (in0.Name = in1.Name))

)

SELECT *

FROM Join_101_inner
