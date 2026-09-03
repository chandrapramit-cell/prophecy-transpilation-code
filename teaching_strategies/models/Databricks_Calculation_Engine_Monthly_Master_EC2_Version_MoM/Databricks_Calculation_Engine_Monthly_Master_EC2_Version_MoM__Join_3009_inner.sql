{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH RecordID_3003 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__RecordID_3003')}}

),

Join_2437_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2437_left_UnionLeftOuter')}}

),

Summarize_3004 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_3004')}}

),

Join_3005_left AS (

  SELECT in0.*
  
  FROM Summarize_3004 AS in0
  ANTI JOIN Join_2437_left_UnionLeftOuter AS in1
     ON (
      (
        (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Account Name: Mas90 Customer Number`)
        AND (in0.`Product Code` = in1.`Product Code`)
      )
      AND (in0.`Order: Sales Order Number` = in1.`Renewed Contract: Order: Sales Order Number`)
    )

),

Join_3006_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Order: Account Name: Mas90 Customer Number`, 
    `Product Code`, 
    `Order: Sales Order Number`, 
    `Original_StartDate_Annualization`, 
    `Original_EndDate_Annualization`, 
    `Sum_Quantity`, 
    `Orders ACV`, 
    `Product`)
  
  FROM RecordID_3003 AS in0
  INNER JOIN Join_3005_left AS in1
     ON (
      (
        (
          (
            (
              (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Order: Account Name: Mas90 Customer Number`)
              AND (in0.`Product Code` = in1.`Product Code`)
            )
            AND (in0.`Order: Sales Order Number` = in1.`Order: Sales Order Number`)
          )
          AND (in0.Product = in1.Product)
        )
        AND (in0.StartDate_Annualization = in1.Original_StartDate_Annualization)
      )
      AND (in0.EndDate_Annualization = in1.Original_EndDate_Annualization)
    )

),

AlteryxSelect_3008 AS (

  SELECT 
    `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Product Code` AS `Product Code`,
    `Order: Order` AS `Order: Order`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Order: Start Date` AS `Order: Start Date`,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    `Sum_Total Price (new)` AS `Sum_Total Price (new)`,
    Sum_Quantity AS Sum_Quantity,
    `Item/Product` AS `Item/Product`,
    Product AS Product,
    StartDate_Annualization AS StartDate_Annualization,
    EndDate_Annualization AS EndDate_Annualization,
    ACV AS ACV,
    TCV AS TCV,
    `Order: Opportunity: Opportunity Name` AS `Order: Opportunity: Opportunity Name`,
    * EXCEPT (`RecordID`, 
    `Renewal_StartDate_Annualization`, 
    `Renewal_EndDate_Annualization`, 
    `Renewal_Order: Activated Date`, 
    `Renewal_Order: Order`, 
    `Renewed_Order: Opportunity: Renewed Contract: Order: Order`, 
    `TS_ContractDays`, 
    `Engine_ContractDays`, 
    `PreReductions_Total Price`, 
    `Reduction Amount`, 
    `Order: Account Name: Mas90 Customer Number`, 
    `Product Code`, 
    `Order: Order`, 
    `Order: Sales Order Number`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    `Order: Activated Date`, 
    `Order: Start Date`, 
    `Order: End Date (Calculated)`, 
    `Order: Subscription Term`, 
    `Sum_Total Price (new)`, 
    `Sum_Quantity`, 
    `Item/Product`, 
    `Product`, 
    `StartDate_Annualization`, 
    `EndDate_Annualization`, 
    `ACV`, 
    `TCV`, 
    `Order: Opportunity: Opportunity Name`)
  
  FROM Join_3006_inner AS in0

),

AlteryxSelect_1046 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_1046')}}

),

Join_3009_inner AS (

  SELECT 
    in1.Product AS Right_Product,
    in1.StartDate_Annualization AS Right_StartDate_Annualization,
    in1.EndDate_Annualization AS Right_EndDate_Annualization,
    in1.ACV AS Right_ACV,
    in1.TCV AS Right_TCV,
    in0.* EXCEPT (`Product Code`),
    in1.* EXCEPT (`Product`, `StartDate_Annualization`, `EndDate_Annualization`, `ACV`, `TCV`)
  
  FROM AlteryxSelect_3008 AS in0
  INNER JOIN AlteryxSelect_1046 AS in1
     ON (
      (
        (in0.`Product Code` = in1.`Product Code`)
        AND (in0.`Order: Sales Order Number` = in1.`Renewed Contract: Order: Sales Order Number`)
      )
      AND (in0.EndDate_Annualization = in1.Original_EndDate_Annualization)
    )

)

SELECT *

FROM Join_3009_inner
