{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_1183 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1183')}}

),

Summarize_1660 AS (

  SELECT 
    MODE(`Customer Segment`) AS `Mode_Customer Segment`,
    CustomerName AS CustomerName,
    RevMonth AS RevMonth
  
  FROM Formula_1183 AS in0
  
  GROUP BY 
    CustomerName, RevMonth

)

SELECT *

FROM Summarize_1660
