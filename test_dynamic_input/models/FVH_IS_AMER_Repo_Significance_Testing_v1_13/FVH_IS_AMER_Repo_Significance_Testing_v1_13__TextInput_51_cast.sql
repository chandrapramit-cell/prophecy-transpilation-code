{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_51 AS (

  SELECT * 
  
  FROM {{ ref('seed_FVH_IS_AMER_Repo_Significance_Testing_v1_13_51')}}

),

TextInput_51_cast AS (

  SELECT 
    CAST(Tenor AS string) AS Tenor,
    CAST(`Tenor Map` AS INTEGER) AS `Tenor Map`,
    CAST(`VA Map` AS string) AS `VA Map`
  
  FROM TextInput_51 AS in0

)

SELECT *

FROM TextInput_51_cast
