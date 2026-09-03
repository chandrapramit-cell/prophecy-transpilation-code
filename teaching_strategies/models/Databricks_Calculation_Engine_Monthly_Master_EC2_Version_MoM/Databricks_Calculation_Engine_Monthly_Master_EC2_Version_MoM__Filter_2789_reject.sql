{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_2787 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2787')}}

),

Filter_2789_reject AS (

  SELECT * 
  
  FROM Formula_2787 AS in0
  
  WHERE (NOT (isnull(`Change Category`)) OR isnull(isnull(`Change Category`)))

)

SELECT *

FROM Filter_2789_reject
