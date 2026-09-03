{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_3128 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_3128')}}

),

Join_1012_right AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1012_right')}}

),

Join_2969_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Mas90 Customer Number`)
  
  FROM Join_1012_right AS in0
  INNER JOIN Summarize_3128 AS in1
     ON (in0.`Account Name: Mas90 Customer Number` = in1.`Mas90 Customer Number`)

)

SELECT *

FROM Join_2969_inner
