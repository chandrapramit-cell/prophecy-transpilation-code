{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Filter_588 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_588')}}

),

Summarize_2597 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2597')}}

),

Join_595_left AS (

  SELECT in0.*
  
  FROM Filter_588 AS in0
  ANTI JOIN Summarize_2597 AS in1
     ON (in0.`Product Code` = in1.`Item/Product`)

)

SELECT *

FROM Join_595_left
