{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_153_3124_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_153_3124_inner')}}

),

GenerateRows_162_3124 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Join_153_3124_inner'], 
      '[{"name": "RecordIDExit", "dataType": "Integer"}, {"name": "Duplication Safeguard Flag", "dataType": "Boolean"}, {"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}, {"name": "Original Amount", "dataType": "Double"}]', 
      'last_day(payload.StartDate_Annualization)', 
      '(ARRMonth <= payload.EndDate_Annualization)', 
      'last_day(add_months(ARRMonth, 1))', 
      'ARRMonth', 
      '100', 
      'recursive'
    )
  }}

),

Summarize_163_3124 AS (

  SELECT 
    DISTINCT `Mas90 Customer Number` AS `Mas90 Customer Number`,
    Product AS Product,
    ARRMonth AS ARRMonth,
    MacroRecordID AS MacroRecordID
  
  FROM GenerateRows_162_3124 AS in0

)

SELECT *

FROM Summarize_163_3124
