{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_150_3124_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_150_3124_inner')}}

),

AlteryxSelect_25_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__AlteryxSelect_25_3124')}}

),

Join_152_3124_inner AS (

  SELECT 
    in1.RecordID AS Right_RecordID,
    in1.ManualRecordID AS Right_ManualRecordID,
    in1.`Mas90 Customer Number` AS `Right_Mas90 Customer Number`,
    in1.Product AS Right_Product,
    in1.`Product Code` AS `Right_Product Code`,
    in1.`Order: Sales Order Number` AS `Right_Order: Sales Order Number`,
    in1.`Order: Opportunity: Renewed Contract: Order: Order` AS `Right_Order: Opportunity: Renewed Contract: Order: Order`,
    in1.StartDate_Annualization AS Right_StartDate_Annualization,
    in1.EndDate_Annualization AS Right_EndDate_Annualization,
    in1.Manual_StartDate_Annualization AS Right_Manual_StartDate_Annualization,
    in1.Manual_EndDate_Annualization AS Right_Manual_EndDate_Annualization,
    in1.`Order: Start Date` AS `Right_Order: Start Date`,
    in1.`Order: End Date (Calculated)` AS `Right_Order: End Date (Calculated)`,
    in1.`Order: Subscription Term` AS `Right_Order: Subscription Term`,
    in1.`Order: Order` AS `Right_Order: Order`,
    in1.`PreReductions_Total Price` AS `Right_PreReductions_Total Price`,
    in1.`Sum_Total Price (new)` AS `Right_Sum_Total Price (new)`,
    in1.ACV AS Right_ACV,
    in1.TCV AS Right_TCV,
    in1.Quantity AS Right_Quantity,
    in1.`Order: Activated Date` AS `Right_Order: Activated Date`,
    in1.`Actual Closed Date` AS `Right_Actual Closed Date`,
    in1.Origin AS Right_Origin,
    in1.`Expected Renewal Date` AS `Right_Expected Renewal Date`,
    in1.`Created Date` AS `Right_Created Date`,
    in1.Stage AS Right_Stage,
    in1.MacroRecordID AS Right_MacroRecordID,
    in1.`Date Overlap Flag` AS `Right_Date Overlap Flag`,
    in1.`Filtering Review` AS `Right_Filtering Review`,
    in1.Original_StartDate_Annualization AS Right_Original_StartDate_Annualization,
    in1.Original_EndDate_Annualization AS Right_Original_EndDate_Annualization,
    in1.TS_ContractDays AS Right_TS_ContractDays,
    in1.Engine_ContractDays AS Right_Engine_ContractDays,
    in1.`Original Amount` AS `Right_Original Amount`,
    in1.`Duplication Safeguard Flag` AS `Right_Duplication Safeguard Flag`,
    in1.RecordIDExit AS Right_RecordIDExit,
    in0.* EXCEPT (`F9`, `ContractTermDays`, `Customer Data ARR`, `Notes`, `prophecy_recordId_564`),
    in1.* EXCEPT (`RecordID`, 
    `ManualRecordID`, 
    `Mas90 Customer Number`, 
    `Product`, 
    `Product Code`, 
    `Order: Sales Order Number`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    `StartDate_Annualization`, 
    `EndDate_Annualization`, 
    `Manual_StartDate_Annualization`, 
    `Manual_EndDate_Annualization`, 
    `Order: Start Date`, 
    `Order: End Date (Calculated)`, 
    `Order: Subscription Term`, 
    `Order: Order`, 
    `PreReductions_Total Price`, 
    `Sum_Total Price (new)`, 
    `ACV`, 
    `TCV`, 
    `Quantity`, 
    `Order: Activated Date`, 
    `Actual Closed Date`, 
    `Origin`, 
    `Expected Renewal Date`, 
    `Created Date`, 
    `Stage`, 
    `MacroRecordID`, 
    `Date Overlap Flag`, 
    `Filtering Review`, 
    `Original_StartDate_Annualization`, 
    `Original_EndDate_Annualization`, 
    `TS_ContractDays`, 
    `Engine_ContractDays`, 
    `Original Amount`, 
    `Duplication Safeguard Flag`, 
    `RecordIDExit`)
  
  FROM Join_150_3124_inner AS in0
  INNER JOIN AlteryxSelect_25_3124 AS in1
     ON (in0.RecordIDExit = in1.RecordIDExit)

)

SELECT *

FROM Join_152_3124_inner
