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

Filter_2914_reject AS (

  SELECT * 
  
  FROM Formula_2872_to_Formula_2871_0 AS in0
  
  WHERE (NOT (isnull(`Change Category`)) OR isnull(isnull(`Change Category`)))

)

SELECT *

FROM Filter_2914_reject
