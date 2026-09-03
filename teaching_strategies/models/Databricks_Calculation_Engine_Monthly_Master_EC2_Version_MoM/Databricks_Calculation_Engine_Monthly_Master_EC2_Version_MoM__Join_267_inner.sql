{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_608 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_608')}}

),

Join_1016_left AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1016_left')}}

),

Join_267_inner AS (

  SELECT 
    in0.`Account Name: Mas90 Customer Number` AS `Account Name: Mas90 Customer Number`,
    in0.`Primary Quote: End Date (Calculated)` AS `Primary Quote: End Date (Calculated)`,
    in0.`Primary Quote: Start Date` AS `Primary Quote: Start Date`,
    in0.`Product Code` AS `Product Code`,
    in0.`Renewed Contract: Order: Sales Order Number` AS `Renewed Contract: Order: Sales Order Number`,
    in1.Product AS Product,
    in0.`Opportunity Name` AS `Opportunity Name`,
    in0.Stage AS Stage,
    in0.`Renewed Contract: Contract Number` AS `Renewed Contract: Contract Number`,
    in0.Quantity AS Quantity,
    in0.`Actual Closed Date` AS `Actual Closed Date`,
    in0.`Expected Renewal Date` AS `Expected Renewal Date`,
    in0.`Created Date` AS `Created Date`,
    in0.`Account Name: Mas90 Customer Number` AS `Left_Left_Account Name: Mas90 Customer Number`,
    in0.* EXCEPT (`Account Name: Mas90 Customer Number`, 
    `Primary Quote: End Date (Calculated)`, 
    `Primary Quote: Start Date`, 
    `Product Code`, 
    `Renewed Contract: Order: Sales Order Number`, 
    `Opportunity Name`, 
    `Stage`, 
    `Renewed Contract: Contract Number`, 
    `Quantity`, 
    `Actual Closed Date`, 
    `Expected Renewal Date`, 
    `Created Date`, 
    `Left_Account Name: Mas90 Customer Number`),
    in1.* EXCEPT (`Item/Product`, `Product`)
  
  FROM Join_1016_left AS in0
  INNER JOIN Summarize_608 AS in1
     ON (in0.`Product Code` = in1.`Item/Product`)

)

SELECT *

FROM Join_267_inner
