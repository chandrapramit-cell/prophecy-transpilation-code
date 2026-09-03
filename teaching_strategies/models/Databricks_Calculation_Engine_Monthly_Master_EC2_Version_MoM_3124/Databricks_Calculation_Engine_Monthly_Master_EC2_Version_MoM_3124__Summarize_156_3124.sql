{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_25_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__AlteryxSelect_25_3124')}}

),

Join_150_3124_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_150_3124_inner')}}

),

Join_152_3124_left AS (

  SELECT in0.*
  
  FROM Join_150_3124_inner AS in0
  ANTI JOIN AlteryxSelect_25_3124 AS in1
     ON (in0.RecordIDExit = in1.RecordIDExit)

),

Summarize_171_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_171_3124')}}

),

Filter_107_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_107_3124')}}

),

Join_170_3124_left AS (

  SELECT in0.*
  
  FROM Filter_107_3124 AS in0
  ANTI JOIN Summarize_171_3124 AS in1
     ON ((in0.`Mas90 Customer Number` = in1.`Mas90 Customer Number`) AND (in0.Product = in1.Product))

),

Filter_135_3124_reject AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_135_3124_reject')}}

),

Union_33_3124 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_135_3124_reject', 'Join_152_3124_left', 'Join_170_3124_left'], 
      [
        '[{"name": "RecordIDExit", "dataType": "Integer"}, {"name": "Duplication Safeguard Flag", "dataType": "Boolean"}, {"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}, {"name": "Original Amount", "dataType": "Double"}]', 
        '[{"name": "RecordIDExit", "dataType": "Integer"}, {"name": "Duplication Safeguard Flag", "dataType": "Boolean"}, {"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}, {"name": "Original Amount", "dataType": "Double"}]', 
        '[{"name": "MacroRecordID", "dataType": "Integer"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "F9", "dataType": "Double"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}, {"name": "Order: Start Date", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_156_3124 AS (

  SELECT 
    DISTINCT Quantity AS Quantity,
    `Actual Closed Date` AS `Actual Closed Date`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    `Order: Order` AS `Order: Order`,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    `Created Date` AS `Created Date`,
    StartDate_Annualization AS StartDate_Annualization,
    Origin AS Origin,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    `Order: Activated Date` AS `Order: Activated Date`,
    ACV AS ACV,
    TCV AS TCV,
    `Expected Renewal Date` AS `Expected Renewal Date`,
    `Product Code` AS `Product Code`,
    `Sum_Total Price (new)` AS `Sum_Total Price (new)`,
    `PreReductions_Total Price` AS `PreReductions_Total Price`,
    Product AS Product,
    Stage AS Stage,
    EndDate_Annualization AS EndDate_Annualization,
    MacroRecordID AS MacroRecordID,
    `Order: Start Date` AS `Order: Start Date`
  
  FROM Union_33_3124 AS in0

)

SELECT *

FROM Summarize_156_3124
