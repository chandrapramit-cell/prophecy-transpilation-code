{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH DynamicInput_212 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1_', 'DynamicInput_212') }}

),

Union_41_1 AS (

  SELECT 
    CAST(`Curve Family` AS string) AS prophecy_column_1,
    CAST(`VA Curve` AS string) AS prophecy_column_3
  
  FROM DynamicInput_212 AS in0

),

AlteryxSelect_42 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AlteryxSelect_42')}}

),

Formula_39_0 AS (

  SELECT 
    CAST((CONCAT('USD', F1)) AS string) AS `VA Curve`,
    *
  
  FROM AlteryxSelect_42 AS in0

),

Union_41_0 AS (

  SELECT 
    CAST(F1 AS string) AS prophecy_column_1,
    CAST(`VA Curve` AS string) AS prophecy_column_3,
    CAST(F3 AS DOUBLE) AS prophecy_column_4
  
  FROM Formula_39_0 AS in0

),

Union_41 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_41_0', 'Union_41_1'], 
      [
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_41_postRename AS (

  SELECT 
    prophecy_column_1 AS F1,
    prophecy_column_3 AS `VA Curve`,
    prophecy_column_4 AS F3
  
  FROM Union_41 AS in0

)

SELECT *

FROM Union_41_postRename
