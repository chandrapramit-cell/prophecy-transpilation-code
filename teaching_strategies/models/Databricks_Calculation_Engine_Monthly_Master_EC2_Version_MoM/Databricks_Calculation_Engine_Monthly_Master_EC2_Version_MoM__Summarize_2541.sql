{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_608 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_608')}}

),

Join_1016_left AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1016_left')}}

),

Join_267_left AS (

  SELECT in0.*
  
  FROM Join_1016_left AS in0
  ANTI JOIN Summarize_608 AS in1
     ON (in0.`Product Code` = in1.`Item/Product`)

),

Summarize_2541 AS (

  SELECT DISTINCT `Product Code` AS `Product Code`
  
  FROM Join_267_left AS in0

)

SELECT *

FROM Summarize_2541
