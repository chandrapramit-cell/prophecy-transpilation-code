{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_2782_inner_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2782_inner_UnionLeftOuter')}}

),

Summarize_2813 AS (

  SELECT SUM(Revenue) AS `After Prior Revenue`
  
  FROM Join_2782_inner_UnionLeftOuter AS in0

)

SELECT *

FROM Summarize_2813
