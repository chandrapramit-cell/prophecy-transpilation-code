{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_1972_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1972_0')}}

),

Filter_2309 AS (

  SELECT * 
  
  FROM Formula_1972_0 AS in0
  
  WHERE isnull(StaticHistoryMonth)

)

SELECT *

FROM Filter_2309
