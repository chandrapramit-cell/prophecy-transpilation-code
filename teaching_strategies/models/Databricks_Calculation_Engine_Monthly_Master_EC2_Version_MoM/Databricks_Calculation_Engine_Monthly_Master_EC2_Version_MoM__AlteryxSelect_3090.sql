{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH DbFileInput_308_3089 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DbFileInput_308_3089'
    )
  }}

),

AlteryxSelect_3090 AS (

  SELECT 
    `Loser MAS90` AS `Loser Mas90 Customer Number`,
    `Winner MAS90` AS `Winner Mas90 Customer Number`,
    * EXCEPT (`Loser MAS90`, `Winner MAS90`)
  
  FROM DbFileInput_308_3089 AS in0

)

SELECT *

FROM AlteryxSelect_3090
