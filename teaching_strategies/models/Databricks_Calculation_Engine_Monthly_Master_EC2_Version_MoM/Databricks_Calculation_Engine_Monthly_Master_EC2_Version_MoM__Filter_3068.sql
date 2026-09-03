{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_2621_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2621_inner')}}

),

Filter_3068 AS (

  SELECT * 
  
  FROM Join_2621_inner AS in0
  
  WHERE (`Comparison Method` = 'Month-over-month')

)

SELECT *

FROM Filter_3068
