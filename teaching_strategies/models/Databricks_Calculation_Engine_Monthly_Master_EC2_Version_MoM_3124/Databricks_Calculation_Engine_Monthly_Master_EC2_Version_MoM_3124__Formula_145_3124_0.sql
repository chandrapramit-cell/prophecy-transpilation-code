{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH RecordID_148_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_148_3124')}}

),

Filter_135_3124 AS (

  SELECT * 
  
  FROM RecordID_148_3124 AS in0
  
  WHERE (ManualRecordID IS NULL)

),

Filter_135_3124_reject AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_135_3124_reject')}}

),

Summarize_138_3124 AS (

  SELECT 
    DISTINCT `Mas90 Customer Number` AS `Mas90 Customer Number`,
    Product AS Product,
    StartDate_Annualization AS StartDate_Annualization,
    EndDate_Annualization AS EndDate_Annualization
  
  FROM Filter_135_3124_reject AS in0

),

Join_134_3124_inner AS (

  SELECT 
    in0.RecordID AS RecordID,
    in0.ManualRecordID AS ManualRecordID,
    in0.`Mas90 Customer Number` AS `Mas90 Customer Number`,
    in0.Product AS Product,
    in0.`Product Code` AS `Product Code`,
    in0.`Order: Sales Order Number` AS `Order: Sales Order Number`,
    in0.`Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    in0.StartDate_Annualization AS StartDate_Annualization,
    in0.EndDate_Annualization AS EndDate_Annualization,
    in1.StartDate_Annualization AS Manual_StartDate_Annualization,
    in1.EndDate_Annualization AS Manual_EndDate_Annualization,
    in0.`Order: Start Date` AS `Order: Start Date`,
    in0.`Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    in0.`Order: Subscription Term` AS `Order: Subscription Term`,
    in0.`Order: Order` AS `Order: Order`,
    in0.`PreReductions_Total Price` AS `PreReductions_Total Price`,
    in0.`Sum_Total Price (new)` AS `Sum_Total Price (new)`,
    in0.ACV AS ACV,
    in0.TCV AS TCV,
    in0.Quantity AS Quantity,
    in0.`Order: Activated Date` AS `Order: Activated Date`,
    in0.`Actual Closed Date` AS `Actual Closed Date`,
    in0.Origin AS Origin,
    in0.`Expected Renewal Date` AS `Expected Renewal Date`,
    in0.`Created Date` AS `Created Date`,
    in0.Stage AS Stage,
    in0.MacroRecordID AS MacroRecordID,
    in0.`Date Overlap Flag` AS `Date Overlap Flag`,
    in0.`Filtering Review` AS `Filtering Review`,
    in0.Original_StartDate_Annualization AS Original_StartDate_Annualization,
    in0.Original_EndDate_Annualization AS Original_EndDate_Annualization,
    in0.TS_ContractDays AS TS_ContractDays,
    in0.Engine_ContractDays AS Engine_ContractDays,
    in0.`Original Amount` AS `Original Amount`,
    in0.`Duplication Safeguard Flag` AS `Duplication Safeguard Flag`,
    in0.RecordIDExit AS RecordIDExit,
    in0.* EXCEPT (`RecordID`, 
    `ManualRecordID`, 
    `Mas90 Customer Number`, 
    `Product`, 
    `Product Code`, 
    `Order: Sales Order Number`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    `StartDate_Annualization`, 
    `EndDate_Annualization`, 
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
    `RecordIDExit`, 
    `Manual_StartDate_Annualization`, 
    `Manual_EndDate_Annualization`),
    in1.* EXCEPT (`StartDate_Annualization`, `EndDate_Annualization`, `Mas90 Customer Number`, `Product`)
  
  FROM Filter_135_3124 AS in0
  INNER JOIN Summarize_138_3124 AS in1
     ON ((in0.`Mas90 Customer Number` = in1.`Mas90 Customer Number`) AND (in0.Product = in1.Product))

),

Formula_145_3124_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (StartDate_Annualization <= Manual_EndDate_Annualization)
          AND (EndDate_Annualization >= Manual_StartDate_Annualization)
        )
          THEN 1
        ELSE 0
      END
    ) AS BOOLEAN) AS `Date Overlap Flag`,
    * EXCEPT (`date overlap flag`)
  
  FROM Join_134_3124_inner AS in0

)

SELECT *

FROM Formula_145_3124_0
