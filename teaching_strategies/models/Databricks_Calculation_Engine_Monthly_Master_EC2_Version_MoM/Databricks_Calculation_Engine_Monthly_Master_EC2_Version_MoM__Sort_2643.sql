{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_2820_to_Formula_2806_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2820_to_Formula_2806_0')}}

),

Filter_2640 AS (

  SELECT * 
  
  FROM Formula_2820_to_Formula_2806_0 AS in0
  
  WHERE (`Comparison Method` = 'Month-over-Month')

),

Summarize_2641 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    RevMonth AS RevMonth
  
  FROM Filter_2640 AS in0
  
  GROUP BY RevMonth

),

Sort_2643 AS (

  SELECT * 
  
  FROM Summarize_2641 AS in0
  
  ORDER BY RevMonth ASC

)

SELECT *

FROM Sort_2643
