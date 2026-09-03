{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH TextInput_596 AS (

  SELECT * 
  
  FROM {{ ref('seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_596')}}

),

TextInput_596_cast AS (

  SELECT CAST(`Item/Product` AS string) AS `Item/Product`
  
  FROM TextInput_596 AS in0

)

SELECT *

FROM TextInput_596_cast
