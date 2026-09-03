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

Filter_3067 AS (

  SELECT * 
  
  FROM Filter_3068 AS in0
  
  WHERE (UPPER(`Customer Level Flag`) = UPPER('Y'))

),

Summarize_3069 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    `Revenue Period` AS `Revenue Period`
  
  FROM Filter_3067 AS in0
  
  GROUP BY `Revenue Period`

),

Sort_3064 AS (

  SELECT * 
  
  FROM Summarize_3069 AS in0
  
  ORDER BY `Revenue Period` ASC

)

SELECT *

FROM Sort_3064
