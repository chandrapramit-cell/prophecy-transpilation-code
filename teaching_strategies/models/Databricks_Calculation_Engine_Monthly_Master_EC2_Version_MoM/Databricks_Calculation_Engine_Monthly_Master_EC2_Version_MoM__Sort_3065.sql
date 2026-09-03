{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Filter_3068 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3068')}}

),

Summarize_3066 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    `Revenue Period` AS `Revenue Period`,
    Product AS Product
  
  FROM Filter_3068 AS in0
  
  GROUP BY 
    `Revenue Period`, Product

),

Sort_3065 AS (

  SELECT * 
  
  FROM Summarize_3066 AS in0
  
  ORDER BY Product ASC, `Revenue Period` ASC

)

SELECT *

FROM Sort_3065
