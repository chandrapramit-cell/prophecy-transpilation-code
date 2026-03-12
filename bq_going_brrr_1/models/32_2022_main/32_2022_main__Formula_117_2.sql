{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_21_2 AS (

  SELECT *
  
  FROM {{ ref('32_2022_main__Formula_21_2')}}

),

AlteryxSelect_66 AS (

  SELECT 
    Team AS Team,
    Match AS Match,
    ClimbLowAttempted AS ClimbLowAttempted,
    ClimbLowSuccess AS ClimbLowSuccess,
    ClimbMidAttempted AS ClimbMidAttempted,
    ClimbMidSuccess AS ClimbMidSuccess,
    ClimbHighAttempted AS ClimbHighAttempted,
    ClimbHighSuccess AS ClimbHighSuccess,
    ClimbTraversalAttempted AS ClimbTraversalAttempted,
    ClimbTraversalSuccess AS ClimbTraversalSuccess,
    climbLowTime AS climbLowTime,
    climbMidTime AS climbMidTime,
    climbHighTime AS climbHighTime,
    climbTraversalTime AS climbTraversalTime,
    climbTotalTime AS climbTotalTime
  
  FROM Formula_21_2 AS in0

),

MultiRowFormula_70_row_id_drop_0 AS (

  SELECT *
  
  FROM {{ ref('32_2022_main__MultiRowFormula_70_row_id_drop_0')}}

),

Formula_7_0 AS (

  SELECT 
    (AutoUpperSuccess + TeleUpperSuccess) AS TotalUpperSuccess,
    (AutoLowerSuccess + TeleLowerSuccess) AS TotalLowerSuccess,
    *
  
  FROM MultiRowFormula_70_row_id_drop_0 AS in0

),

Formula_7_1 AS (

  SELECT 
    (TotalUpperSuccess + TotalLowerSuccess) AS TotalSuccess,
    (
      (
        (((TotalUpperSuccess + TotalLowerSuccess) + AutoUpperFailures) + AutoLowerFailures)
        + TeleUpperFailures
      )
      + TeleLowerFailures
    ) AS TotalShots,
    CAST((
      CONCAT(
        'M', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(Match AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        ' - ', 
        Comment)
    ) AS STRING) AS CommentWithMatch,
    (((AutoLowerSuccess * 2) + (AutoUpperSuccess * 4)) + (Taxi * 5)) AS AutoPoints,
    (TeleLowerSuccess + (TeleUpperSuccess * 2)) AS TelePoints,
    *
  
  FROM Formula_7_0 AS in0

),

Join_67_inner AS (

  SELECT 
    in1.Team AS Right_Team,
    in1.Match AS Right_Match,
    in0.*,
    in1.* EXCEPT (`Team`, `Match`)
  
  FROM Formula_7_1 AS in0
  INNER JOIN AlteryxSelect_66 AS in1
     ON ((in0.Team = in1.Team) AND (in0.Match = in1.Match))

),

Formula_117_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (ClimbMidSuccess = 0)
          THEN (ClimbLowSuccess * 4)
        ELSE 0
      END
    ) AS INT64) AS ClimbLowPoints,
    CAST((
      CASE
        WHEN (ClimbHighSuccess = 0)
          THEN (ClimbMidSuccess * 6)
        ELSE 0
      END
    ) AS INT64) AS ClimbMidPoints,
    CAST((
      CASE
        WHEN (ClimbTraversalSuccess = 0)
          THEN (ClimbHighSuccess * 10)
        ELSE 0
      END
    ) AS INT64) AS ClimbHighPoints,
    CAST((ClimbTraversalSuccess * 15) AS INT64) AS ClimbTraversalPoints,
    *
  
  FROM Join_67_inner AS in0

),

Formula_117_1 AS (

  SELECT 
    (((ClimbLowPoints + ClimbMidPoints) + ClimbHighPoints) + ClimbTraversalPoints) AS TotalClimbPoints,
    *
  
  FROM Formula_117_0 AS in0

),

Formula_117_2 AS (

  SELECT 
    ((TotalClimbPoints + TelePoints) + AutoPoints) AS TotalPoints,
    *
  
  FROM Formula_117_1 AS in0

)

SELECT *

FROM Formula_117_2
