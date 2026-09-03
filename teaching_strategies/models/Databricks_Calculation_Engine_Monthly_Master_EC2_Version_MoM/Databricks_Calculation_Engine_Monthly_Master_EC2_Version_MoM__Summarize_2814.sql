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

Summarize_2814 AS (

  SELECT SUM(Revenue) AS `After Initial Amounts`
  
  FROM Join_2793_left_UnionLeftOuter AS in0

)

SELECT *

FROM Summarize_2814
