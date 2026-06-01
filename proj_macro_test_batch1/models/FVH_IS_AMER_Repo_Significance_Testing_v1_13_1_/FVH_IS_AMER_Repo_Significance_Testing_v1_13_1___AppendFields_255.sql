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

Join_142_inner AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_142_inner')}}

),

Summarize_144 AS (

  SELECT SUM(Sum_MTM) AS Sum_Sum_MTM
  
  FROM Join_142_inner AS in0

),

Union_197_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_197_postRename')}}

),

Summarize_145 AS (

  SELECT SUM(MTM) AS Sum_MTM
  
  FROM Union_197_postRename AS in0

),

AppendFields_148 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_145 AS in0
  INNER JOIN Summarize_144 AS in1
     ON TRUE

),

Formula_146_0 AS (

  SELECT 
    CAST((Sum_Sum_MTM - Sum_MTM) AS DOUBLE) AS `Check`,
    *
  
  FROM AppendFields_148 AS in0

),

AppendFields_255 AS (

  SELECT 
    in1.Sum_Sum_MTM AS Sum_Sum_MTM,
    in1.Sum_MTM AS Sum_MTM,
    in1.Check AS `Check`,
    in0.`MTM Mismatch` AS `MTM Mismatch`
  
  FROM Formula_235_0 AS in0
  INNER JOIN Formula_146_0 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_255
