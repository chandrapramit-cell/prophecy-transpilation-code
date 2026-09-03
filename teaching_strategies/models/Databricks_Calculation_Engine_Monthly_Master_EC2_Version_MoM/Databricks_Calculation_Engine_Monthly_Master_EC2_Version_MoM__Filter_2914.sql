{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_2872_to_Formula_2871_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2872_to_Formula_2871_0')}}

),

Filter_2914 AS (

  SELECT * 
  
  FROM Formula_2872_to_Formula_2871_0 AS in0
  
  WHERE isnull(`Change Category`)

)

SELECT *

FROM Filter_2914
