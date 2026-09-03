{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH TextInput_596_cast AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TextInput_596_cast')}}

),

Summarize_2597 AS (

  SELECT DISTINCT `Item/Product` AS `Item/Product`
  
  FROM TextInput_596_cast AS in0

)

SELECT *

FROM Summarize_2597
