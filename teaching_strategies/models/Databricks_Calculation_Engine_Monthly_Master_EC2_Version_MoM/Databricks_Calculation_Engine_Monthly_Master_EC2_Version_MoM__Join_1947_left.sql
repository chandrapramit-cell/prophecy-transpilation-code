{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH TextInput_1948_cast AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TextInput_1948_cast')}}

),

Union_1980 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_1980')}}

),

Join_1947_left AS (

  SELECT in0.*
  
  FROM Union_1980 AS in0
  ANTI JOIN TextInput_1948_cast AS in1
     ON (in0.Product = in1.Product)

)

SELECT *

FROM Join_1947_left
