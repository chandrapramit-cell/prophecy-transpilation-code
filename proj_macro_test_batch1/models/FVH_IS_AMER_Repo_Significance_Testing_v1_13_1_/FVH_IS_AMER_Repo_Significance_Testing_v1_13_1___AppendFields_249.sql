{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_235_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_235_0')}}

),

Union_197_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_197_postRename')}}

),

AppendFields_249 AS (

  SELECT 
    in1.Portfolio AS Portfolio,
    in0.Result AS Result,
    in1.`Override paranthesesOpenYslashNparanthesesClose` AS `Override paranthesesOpenYslashNparanthesesClose`,
    in1.TradeID AS TradeID,
    in1.Region AS Region,
    in1.Sum_Impact AS Sum_Impact,
    in1.SignificanceTest AS SignificanceTest,
    in1.MTM AS MTM,
    in1.Level AS Level
  
  FROM Formula_235_0 AS in0
  INNER JOIN Union_197_postRename AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_249
