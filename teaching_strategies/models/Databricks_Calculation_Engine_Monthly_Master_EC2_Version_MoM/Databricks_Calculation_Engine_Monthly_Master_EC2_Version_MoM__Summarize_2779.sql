{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_2780 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2780')}}

),

Filter_2783 AS (

  SELECT * 
  
  FROM Summarize_2780 AS in0
  
  WHERE (Revenue > 0)

),

Summarize_2779 AS (

  SELECT 
    MIN(RevMonth) AS First_PosRevMonth,
    MAX(RevMonth) AS Last_PosRevMonth,
    CustomerName AS CustomerName
  
  FROM Filter_2783 AS in0
  
  GROUP BY CustomerName

)

SELECT *

FROM Summarize_2779
