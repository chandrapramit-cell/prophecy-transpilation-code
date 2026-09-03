{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_2786_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2786_left_UnionLeftOuter')}}

),

Filter_2807_reject AS (

  SELECT * 
  
  FROM Join_2786_left_UnionLeftOuter AS in0
  
  WHERE (
          NOT (((RevMonth >= First_NonZeroRevMonth) OR (to_date(`Previous Period`) <= Last_NonZeroRevMonth)))
          OR isnull(((RevMonth >= First_NonZeroRevMonth) OR (to_date(`Previous Period`) <= Last_NonZeroRevMonth)))
        )

),

Summarize_2812 AS (

  SELECT SUM(Revenue) AS Sum_Revenue
  
  FROM Filter_2807_reject AS in0

)

SELECT *

FROM Summarize_2812
