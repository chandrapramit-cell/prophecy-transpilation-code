{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_595_left AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_595_left')}}

),

Summarize_608 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_608')}}

),

Join_594_left AS (

  SELECT in0.*
  
  FROM Join_595_left AS in0
  ANTI JOIN Summarize_608 AS in1
     ON (in0.`Product Code` = in1.`Item/Product`)

),

Summarize_2940 AS (

  SELECT 
    SUM(`Sum_Total Price (new)`) AS `Sum_Sum_Total Price (new)`,
    `Product Code` AS `Product Code`
  
  FROM Join_594_left AS in0
  
  GROUP BY `Product Code`

)

SELECT *

FROM Summarize_2940
