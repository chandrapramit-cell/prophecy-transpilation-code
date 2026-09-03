{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH TextInput_1948 AS (

  SELECT * 
  
  FROM {{ ref('seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_1948')}}

),

TextInput_1948_cast AS (

  SELECT 
    CAST(Product AS string) AS Product,
    CAST(`Clean Product` AS string) AS `Clean Product`
  
  FROM TextInput_1948 AS in0

)

SELECT *

FROM TextInput_1948_cast
