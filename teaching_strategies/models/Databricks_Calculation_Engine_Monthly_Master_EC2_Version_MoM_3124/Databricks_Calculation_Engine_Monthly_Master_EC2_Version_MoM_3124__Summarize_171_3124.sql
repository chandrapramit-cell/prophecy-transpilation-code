{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Filter_107_3124_reject AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_107_3124_reject')}}

),

Summarize_171_3124 AS (

  SELECT 
    DISTINCT `Mas90 Customer Number` AS `Mas90 Customer Number`,
    Product AS Product
  
  FROM Filter_107_3124_reject AS in0

)

SELECT *

FROM Summarize_171_3124
