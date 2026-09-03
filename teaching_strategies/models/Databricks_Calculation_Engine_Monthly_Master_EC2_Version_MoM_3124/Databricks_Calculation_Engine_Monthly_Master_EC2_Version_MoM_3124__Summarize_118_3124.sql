{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Filter_107_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_107_3124')}}

),

GenerateRows_117_3124 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Filter_107_3124'], 
      '[{"name": "MacroRecordID", "dataType": "Integer"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "F9", "dataType": "Double"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}, {"name": "Order: Start Date", "dataType": "String"}]', 
      'last_day(payload.StartDate_Annualization)', 
      '(ARRMonth <= payload.EndDate_Annualization)', 
      'last_day(add_months(ARRMonth, 1))', 
      'ARRMonth', 
      '100', 
      'recursive'
    )
  }}

),

Summarize_118_3124 AS (

  SELECT 
    DISTINCT `Mas90 Customer Number` AS `Mas90 Customer Number`,
    RecordID AS RecordID,
    Product AS Product,
    ARRMonth AS ARRMonth
  
  FROM GenerateRows_117_3124 AS in0

)

SELECT *

FROM Summarize_118_3124
