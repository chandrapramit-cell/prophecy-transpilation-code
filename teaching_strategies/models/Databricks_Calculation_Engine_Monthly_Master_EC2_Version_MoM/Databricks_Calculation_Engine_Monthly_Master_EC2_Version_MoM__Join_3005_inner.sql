{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_3004 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_3004')}}

),

Join_2437_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2437_left_UnionLeftOuter')}}

),

Join_3005_inner AS (

  SELECT 
    in0.`Orders ACV` AS `Orders ACV`,
    in0.Sum_Quantity AS `Orders Quantity`,
    in0.Original_EndDate_Annualization AS Original_EndDate_Annualization,
    in1.`Account Name: Mas90 Customer Number` AS `Account Name: Mas90 Customer Number`,
    in1.`Primary Quote: End Date (Calculated)` AS `Primary Quote: End Date (Calculated)`,
    in1.`Primary Quote: Start Date` AS `Primary Quote: Start Date`,
    in1.Product AS Product,
    in1.`Product Code` AS `Product Code`,
    in1.`Renewed Contract: Order: Sales Order Number` AS `Renewed Contract: Order: Sales Order Number`,
    in1.`Opportunity Name` AS `Opportunity Name`,
    in1.Stage AS Stage,
    in1.`Renewed Contract: Contract Number` AS `Renewed Contract: Contract Number`,
    in1.Quantity AS Quantity,
    in1.`Expected Renewal Date` AS `Expected Renewal Date`,
    in1.`Actual Closed Date` AS `Actual Closed Date`,
    in1.`Created Date` AS `Created Date`,
    in0.* EXCEPT (`Order: Account Name: Mas90 Customer Number`, 
    `Order: Sales Order Number`, 
    `Orders ACV`, 
    `Sum_Quantity`, 
    `Original_EndDate_Annualization`, 
    `Original_StartDate_Annualization`, 
    `Product`, 
    `Product Code`),
    in1.* EXCEPT (`Account Name: Mas90 Customer Number`, 
    `Primary Quote: End Date (Calculated)`, 
    `Primary Quote: Start Date`, 
    `Product`, 
    `Product Code`, 
    `Renewed Contract: Order: Sales Order Number`, 
    `Opportunity Name`, 
    `Stage`, 
    `Renewed Contract: Contract Number`, 
    `Quantity`, 
    `Expected Renewal Date`, 
    `Actual Closed Date`, 
    `Created Date`)
  
  FROM Summarize_3004 AS in0
  INNER JOIN Join_2437_left_UnionLeftOuter AS in1
     ON (
      (
        (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Account Name: Mas90 Customer Number`)
        AND (in0.`Product Code` = in1.`Product Code`)
      )
      AND (in0.`Order: Sales Order Number` = in1.`Renewed Contract: Order: Sales Order Number`)
    )

)

SELECT *

FROM Join_3005_inner
