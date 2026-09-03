{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_267_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_267_inner')}}

),

Filter_1840 AS (

  SELECT * 
  
  FROM Join_267_inner AS in0
  
  WHERE (`Renewed Contract: Order: Sales Order Number` IS NULL)

)

SELECT *

FROM Filter_1840
