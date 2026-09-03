{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_1027 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1027')}}

),

Join_2437_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2437_left_UnionLeftOuter')}}

),

Join_1012_right AS (

  SELECT in0.*
  
  FROM Join_2437_left_UnionLeftOuter AS in0
  ANTI JOIN Summarize_1027 AS in1
     ON (
      (
        (in1.`Order: Account Name: Mas90 Customer Number` = in0.`Account Name: Mas90 Customer Number`)
        AND (in1.`Product Code` = in0.`Product Code`)
      )
      AND (in1.`Order: Sales Order Number` = in0.`Renewed Contract: Order: Sales Order Number`)
    )

)

SELECT *

FROM Join_1012_right
