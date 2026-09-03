{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_2790_to_Formula_2826_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2790_to_Formula_2826_0')}}

),

Summarize_2781 AS (

  SELECT COUNT((
           CASE
             WHEN ((1 IS NULL) OR (CAST(1 AS string) = ''))
               THEN NULL
             ELSE 1
           END
         )) AS `Count`
  
  FROM Formula_2790_to_Formula_2826_0 AS in0

)

SELECT *

FROM Summarize_2781
