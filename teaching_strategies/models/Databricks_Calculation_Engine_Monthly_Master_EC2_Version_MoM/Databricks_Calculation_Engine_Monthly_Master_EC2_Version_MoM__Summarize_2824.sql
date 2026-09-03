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

Summarize_2824 AS (

  SELECT DISTINCT RevMonth AS RevMonth
  
  FROM Formula_2787 AS in0

)

SELECT *

FROM Summarize_2824
