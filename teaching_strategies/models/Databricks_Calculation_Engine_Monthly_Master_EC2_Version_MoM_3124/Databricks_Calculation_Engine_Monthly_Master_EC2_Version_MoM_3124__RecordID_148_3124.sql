{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_116_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_116_3124')}}

),

Summarize_123_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_123_3124')}}

),

Join_121_3124_right AS (

  SELECT in0.*
  
  FROM Summarize_123_3124 AS in0
  ANTI JOIN Summarize_116_3124 AS in1
     ON (
      ((in1.`Mas90 Customer Number` = in0.`Mas90 Customer Number`) AND (in1.Product = in0.Product))
      AND (in1.ARRMonth = in0.ARRMonth)
    )

),

Summarize_124_3124 AS (

  SELECT DISTINCT RecordID AS RecordID
  
  FROM Join_121_3124_right AS in0

),

Filter_126_3124_reject AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_126_3124_reject')}}

),

Join_127_3124_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`RecordID`)
  
  FROM Filter_126_3124_reject AS in0
  INNER JOIN Summarize_124_3124 AS in1
     ON (in0.RecordID = in1.RecordID)

),

RecordID_129_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_129_3124')}}

),

Filter_126_3124 AS (

  SELECT * 
  
  FROM RecordID_129_3124 AS in0
  
  WHERE (UPPER(Origin) = UPPER('Manual Plug In'))

),

Union_128_3124 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_126_3124', 'Join_127_3124_inner'], 
      [
        '[{"name": "RecordID2", "dataType": "Integer"}, {"name": "Duplication Safeguard Flag", "dataType": "Boolean"}, {"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}, {"name": "Original Amount", "dataType": "Double"}]', 
        '[{"name": "RecordID2", "dataType": "Integer"}, {"name": "Duplication Safeguard Flag", "dataType": "Boolean"}, {"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}, {"name": "Original Amount", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_147_3124 AS (

  SELECT * EXCEPT (`RecordID2`)
  
  FROM Union_128_3124 AS in0

),

RecordID_148_3124 AS (

  {{
    prophecy_basics.RecordID(
      ['AlteryxSelect_147_3124'], 
      'incremental_id', 
      'RecordIDExit', 
      'integer', 
      6, 
      1, 
      'groupLevel', 
      'first_column', 
      [], 
      [
        { 'expression': { 'expression': 'RecordID' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'prophecy_recordId_564' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'ManualRecordID' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'MacroRecordID' }, 'sortType': 'asc' }
      ]
    )
  }}

)

SELECT *

FROM RecordID_148_3124
