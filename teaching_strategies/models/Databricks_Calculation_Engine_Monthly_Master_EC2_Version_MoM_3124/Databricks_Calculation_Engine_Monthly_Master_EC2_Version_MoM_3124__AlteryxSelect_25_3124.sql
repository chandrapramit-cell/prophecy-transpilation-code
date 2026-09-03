{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_145_3124_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Formula_145_3124_0')}}

),

Filter_146_3124 AS (

  SELECT * 
  
  FROM Formula_145_3124_0 AS in0
  
  WHERE (`Date Overlap Flag` = TRUE)

),

AlteryxSelect_25_3124 AS (

  SELECT *
  
  FROM Filter_146_3124 AS in0

)

SELECT *

FROM AlteryxSelect_25_3124
