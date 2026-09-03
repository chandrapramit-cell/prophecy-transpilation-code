{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_161_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_161_3124')}}

),

Summarize_163_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_163_3124')}}

),

Join_158_3124_inner AS (

  SELECT 
    in1.`Mas90 Customer Number` AS `Mas90 Customer Number`,
    in1.Product AS Product,
    in1.MacroRecordID AS MacroRecordID,
    in1.ARRMonth AS ARRMonth,
    in0.* EXCEPT (`ARRMonth`, `Mas90 Customer Number`, `Product`),
    in1.* EXCEPT (`Mas90 Customer Number`, `Product`, `MacroRecordID`, `ARRMonth`)
  
  FROM Summarize_161_3124 AS in0
  INNER JOIN Summarize_163_3124 AS in1
     ON (
      ((in0.`Mas90 Customer Number` = in1.`Mas90 Customer Number`) AND (in0.Product = in1.Product))
      AND (in0.ARRMonth = in1.ARRMonth)
    )

)

SELECT *

FROM Join_158_3124_inner
