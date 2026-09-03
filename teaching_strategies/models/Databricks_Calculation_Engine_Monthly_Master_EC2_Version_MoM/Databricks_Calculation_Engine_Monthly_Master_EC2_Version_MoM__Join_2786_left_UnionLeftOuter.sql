{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_2782_inner_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2782_inner_UnionLeftOuter')}}

),

Summarize_2779 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2779')}}

),

Summarize_2780 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_2780')}}

),

Filter_2808 AS (

  SELECT * 
  
  FROM Summarize_2780 AS in0
  
  WHERE (
          NOT(
            Revenue = 0)
        )

),

Summarize_2809 AS (

  SELECT 
    MIN(RevMonth) AS First_NonZeroRevMonth,
    MAX(RevMonth) AS Last_NonZeroRevMonth,
    CustomerName AS CustomerName
  
  FROM Filter_2808 AS in0
  
  GROUP BY CustomerName

),

Join_2810_left_UnionFullOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CustomerName`)
  
  FROM Summarize_2779 AS in0
  FULL JOIN Summarize_2809 AS in1
     ON (in0.CustomerName = in1.CustomerName)

),

Join_2786_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CustomerName`)
  
  FROM Join_2782_inner_UnionLeftOuter AS in0
  LEFT JOIN Join_2810_left_UnionFullOuter AS in1
     ON (in0.CustomerName = in1.CustomerName)

)

SELECT *

FROM Join_2786_left_UnionLeftOuter
