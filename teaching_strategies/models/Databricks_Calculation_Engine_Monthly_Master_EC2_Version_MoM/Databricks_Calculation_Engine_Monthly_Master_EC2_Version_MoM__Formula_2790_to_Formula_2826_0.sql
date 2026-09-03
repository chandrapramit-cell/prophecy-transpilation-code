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

Formula_2790_to_Formula_2826_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Revenue > 0)
          THEN 1
        ELSE 0
      END
    ) AS INTEGER) AS `Customer Active Flag`,
    *
  
  FROM Summarize_2780 AS in0

)

SELECT *

FROM Formula_2790_to_Formula_2826_0
