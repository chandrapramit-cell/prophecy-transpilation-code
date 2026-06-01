{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_41_postRename AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Union_41_postRename')}}

),

Join_190_left AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Join_190_left')}}

),

Join_43_inner AS (

  SELECT 
    in1.F3 AS Right_F3,
    in0.* EXCEPT (`ISIN`),
    in1.* EXCEPT (`F1`, `F3`)
  
  FROM Join_190_left AS in0
  INNER JOIN Union_41_postRename AS in1
     ON (in0.Curve = in1.F1)

),

TextInput_51_cast AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___TextInput_51_cast')}}

),

Join_52_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Tenor`, `Tenor Map`)
  
  FROM Join_43_inner AS in0
  INNER JOIN TextInput_51_cast AS in1
     ON (in0.Tenor = in1.Tenor)

),

Formula_57_0 AS (

  SELECT 
    CAST((CONCAT(`VA Curve`, `VA Map`)) AS string) AS Concat,
    *
  
  FROM Join_52_inner AS in0

)

SELECT *

FROM Formula_57_0
