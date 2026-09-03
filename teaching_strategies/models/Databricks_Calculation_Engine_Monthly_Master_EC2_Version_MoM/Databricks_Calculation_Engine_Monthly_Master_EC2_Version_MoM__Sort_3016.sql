{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_2006_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2006_0')}}

),

Summarize_3015 AS (

  SELECT 
    SUM(ARR) AS Sum_ARR,
    CustomerName AS CustomerName,
    Product AS Product
  
  FROM Formula_2006_0 AS in0
  
  GROUP BY 
    CustomerName, Product

),

Sort_3016 AS (

  SELECT * 
  
  FROM Summarize_3015 AS in0
  
  ORDER BY Sum_ARR DESC

)

SELECT *

FROM Sort_3016
