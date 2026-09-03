{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_3125_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_3125_0')}}

),

Summarize_3128 AS (

  SELECT DISTINCT `Mas90 Customer Number` AS `Mas90 Customer Number`
  
  FROM Formula_3125_0 AS in0

)

SELECT *

FROM Summarize_3128
