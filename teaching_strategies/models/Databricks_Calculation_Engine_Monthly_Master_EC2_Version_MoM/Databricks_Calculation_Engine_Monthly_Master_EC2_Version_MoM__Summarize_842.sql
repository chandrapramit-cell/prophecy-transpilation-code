{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH RecordID_833 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__RecordID_833')}}

),

Formula_829_0 AS (

  SELECT 
    (DATE_ADD(CAST(`Order: Start Date` AS DATE), CAST(-1 AS INTEGER))) AS `Start Date - 1`,
    *
  
  FROM RecordID_833 AS in0

),

Join_831_inner AS (

  SELECT 
    in1.RecordID AS Right_RecordID,
    in1.`Product Code` AS `Right_Product Code`,
    in1.`Order: Order` AS `Right_Order: Order`,
    in1.`Order: Sales Order Number` AS `Right_Order: Sales Order Number`,
    in1.`Order: Subscription Term` AS `Right_Order: Subscription Term`,
    in1.`Sum_Total Price (new)` AS `Right_Sum_Total Price (new)`,
    in1.Sum_Quantity AS Right_Sum_Quantity,
    in1.`Order: Opportunity: Renewed Contract: Order: Order` AS `Right_Order: Opportunity: Renewed Contract: Order: Order`,
    in1.`Item/Product` AS `Right_Item/Product`,
    in1.Product AS Right_Product,
    in1.StartDate_Annualization AS Right_StartDate_Annualization,
    in1.EndDate_Annualization AS Right_EndDate_Annualization,
    in1.ACV AS Right_ACV,
    in1.TCV AS Right_TCV,
    in1.Renewal_StartDate_Annualization AS Right_Renewal_StartDate_Annualization,
    in1.Renewal_EndDate_Annualization AS Right_Renewal_EndDate_Annualization,
    in1.`Renewal_Order: Activated Date` AS `Right_Renewal_Order: Activated Date`,
    in1.`Renewal_Order: Order` AS `Right_Renewal_Order: Order`,
    in1.`Renewed_Order: Opportunity: Renewed Contract: Order: Order` AS `Right_Renewed_Order: Opportunity: Renewed Contract: Order: Order`,
    in1.`Order: Opportunity: Opportunity Name` AS `Right_Order: Opportunity: Opportunity Name`,
    in1.TS_ContractDays AS Right_TS_ContractDays,
    in1.Engine_ContractDays AS Right_Engine_ContractDays,
    in1.`PreReductions_Total Price` AS `Right_PreReductions_Total Price`,
    in1.`Reduction Amount` AS `Right_Reduction Amount`,
    in0.* EXCEPT (`Order: Business Subtype`, `Loser Mas90 Customer Number`, `Order: Opportunity: Actual Closed Date`),
    in1.* EXCEPT (`RecordID`, 
    `Product Code`, 
    `Order: Order`, 
    `Order: Sales Order Number`, 
    `Order: Activated Date`, 
    `Order: Start Date`, 
    `Order: End Date (Calculated)`, 
    `Order: Subscription Term`, 
    `Sum_Total Price (new)`, 
    `Sum_Quantity`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    `Item/Product`, 
    `Product`, 
    `StartDate_Annualization`, 
    `EndDate_Annualization`, 
    `ACV`, 
    `TCV`, 
    `Renewal_StartDate_Annualization`, 
    `Renewal_EndDate_Annualization`, 
    `Renewal_Order: Activated Date`, 
    `Renewal_Order: Order`, 
    `Renewed_Order: Opportunity: Renewed Contract: Order: Order`, 
    `Order: Opportunity: Opportunity Name`, 
    `TS_ContractDays`, 
    `Engine_ContractDays`, 
    `Order: Account Name: Mas90 Customer Number`, 
    `PreReductions_Total Price`, 
    `Reduction Amount`, 
    `Start Date - 1`)
  
  FROM Formula_829_0 AS in0
  INNER JOIN Formula_829_0 AS in1
     ON (
      (CAST(in0.`Order: End Date (Calculated)` AS DATE) = in1.`Start Date - 1`)
      AND (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Order: Account Name: Mas90 Customer Number`)
    )

),

Summarize_832 AS (

  SELECT 
    DISTINCT `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    Right_RecordID AS Right_RecordID,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Start Date - 1` AS `Start Date - 1`,
    RecordID AS RecordID,
    `Order: Start Date` AS `Order: Start Date`
  
  FROM Join_831_inner AS in0

),

Join_831_left AS (

  SELECT in0.*
  
  FROM Formula_829_0 AS in0
  ANTI JOIN Formula_829_0 AS in1
     ON (
      (CAST(in0.`Order: End Date (Calculated)` AS DATE) = in1.`Start Date - 1`)
      AND (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Order: Account Name: Mas90 Customer Number`)
    )

),

Join_834_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`RecordID`, 
    `Order: Account Name: Mas90 Customer Number`, 
    `Order: Activated Date`, 
    `Order: Start Date`, 
    `Order: End Date (Calculated)`, 
    `Start Date - 1`)
  
  FROM Join_831_left AS in0
  INNER JOIN Summarize_832 AS in1
     ON (in0.RecordID = in1.Right_RecordID)

),

Summarize_840 AS (

  SELECT 
    DISTINCT `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Start Date - 1` AS `Start Date - 1`,
    RecordID AS RecordID,
    `Order: Start Date` AS `Order: Start Date`
  
  FROM Join_834_inner AS in0

),

Formula_841_0 AS (

  SELECT 
    CAST(1 AS BOOLEAN) AS `Promotion Flag`,
    *
  
  FROM Summarize_840 AS in0

),

Summarize_839 AS (

  SELECT 
    DISTINCT `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Start Date - 1` AS `Start Date - 1`,
    RecordID AS RecordID,
    `Order: Start Date` AS `Order: Start Date`
  
  FROM Summarize_832 AS in0

),

Formula_837_0 AS (

  SELECT 
    CAST(1 AS BOOLEAN) AS `Promotion Flag`,
    *
  
  FROM Summarize_839 AS in0

),

Union_835 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_837_0', 'Formula_841_0'], 
      [
        '[{"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Start Date - 1", "dataType": "Date"}, {"name": "Promotion Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Start Date - 1", "dataType": "Date"}, {"name": "Promotion Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Order: Start Date", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_843 AS (

  SELECT * 
  
  FROM Union_835 AS in0
  
  WHERE CAST(`Promotion Flag` AS BOOLEAN)

),

Summarize_842 AS (

  SELECT DISTINCT RecordID AS RecordID
  
  FROM Filter_843 AS in0

)

SELECT *

FROM Summarize_842
