{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_2793_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2793_left_UnionLeftOuter')}}

),

Formula_2788 AS (

  SELECT *
  
  FROM Join_2793_left_UnionLeftOuter AS in0

)

SELECT *

FROM Formula_2788
