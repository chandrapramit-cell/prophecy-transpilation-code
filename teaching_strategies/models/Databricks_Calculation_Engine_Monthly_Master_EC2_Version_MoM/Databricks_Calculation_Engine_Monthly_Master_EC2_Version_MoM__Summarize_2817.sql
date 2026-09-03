{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Union_2799 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_2799')}}

),

Summarize_2817 AS (

  SELECT SUM(Revenue) AS `After Segment Migrations`
  
  FROM Union_2799 AS in0

)

SELECT *

FROM Summarize_2817
