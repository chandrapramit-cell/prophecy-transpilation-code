{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_2621_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2621_inner')}}

),

Filter_2644 AS (

  SELECT * 
  
  FROM Join_2621_inner AS in0
  
  WHERE ((`Comparison Method` = 'Year to date') AND (upper(`Customer Level Flag`) = upper('Y')))

),

Summarize_2645 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    `Revenue Period` AS `Revenue Period`
  
  FROM Filter_2644 AS in0
  
  GROUP BY `Revenue Period`

),

Sort_2646 AS (

  SELECT * 
  
  FROM Summarize_2645 AS in0
  
  ORDER BY `Revenue Period` ASC

)

SELECT *

FROM Sort_2646
