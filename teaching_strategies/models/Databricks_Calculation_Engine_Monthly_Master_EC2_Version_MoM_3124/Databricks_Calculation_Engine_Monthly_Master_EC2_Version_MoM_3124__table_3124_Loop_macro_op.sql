{{
  config({    
    "materialized": "table",
    "alias": "table_3124_Loop_macro_op",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_163_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_163_3124')}}

),

Summarize_161_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_161_3124')}}

),

Join_158_3124_right AS (

  SELECT in0.*
  
  FROM Summarize_163_3124 AS in0
  ANTI JOIN Summarize_161_3124 AS in1
     ON (
      ((in1.`Mas90 Customer Number` = in0.`Mas90 Customer Number`) AND (in1.Product = in0.Product))
      AND (in1.ARRMonth = in0.ARRMonth)
    )

),

Summarize_175_3124 AS (

  SELECT DISTINCT MacroRecordID AS MacroRecordID
  
  FROM Join_158_3124_right AS in0

),

table_3124_Engine_Records_macro_ip AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_3124_Engine_Records_macro_ip') }}

),

Sample_174_3124 AS (

  {{
    prophecy_basics.Sample(
      ['table_3124_Engine_Records_macro_ip'], 
      '[{"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "F9", "dataType": "Double"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}, {"name": "Order: Start Date", "dataType": "String"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      1, 
      [
        { 'expression': { 'expression': 'RecordID' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'prophecy_recordId_564' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'ManualRecordID' }, 'sortType': 'asc' }
      ]
    )
  }}

),

Formula_181_3124_0 AS (

  SELECT 
    CAST('REMOVE' AS string) AS `Mas90 Customer Number`,
    * EXCEPT (`mas90 customer number`)
  
  FROM Sample_174_3124 AS in0

),

Union_177_3124_reformat_1 AS (

  SELECT 
    ACV AS ACV,
    `Actual Closed Date` AS `Actual Closed Date`,
    ContractTermDays AS ContractTermDays,
    `Created Date` AS `Created Date`,
    `Customer Data ARR` AS `Customer Data ARR`,
    EndDate_Annualization AS EndDate_Annualization,
    Engine_ContractDays AS Engine_ContractDays,
    `Expected Renewal Date` AS `Expected Renewal Date`,
    F9 AS F9,
    ManualRecordID AS ManualRecordID,
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    Notes AS Notes,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    `Order: Order` AS `Order: Order`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Start Date` AS `Order: Start Date`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    Origin AS Origin,
    `PreReductions_Total Price` AS `PreReductions_Total Price`,
    Product AS Product,
    `Product Code` AS `Product Code`,
    Quantity AS Quantity,
    RecordID AS RecordID,
    Stage AS Stage,
    StartDate_Annualization AS StartDate_Annualization,
    `Sum_Total Price (new)` AS `Sum_Total Price (new)`,
    TCV AS TCV,
    TS_ContractDays AS TS_ContractDays,
    prophecy_recordId_564 AS prophecy_recordId_564
  
  FROM Formula_181_3124_0 AS in0

),

Join_153_3124_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Join_153_3124_inner')}}

),

Join_176_3124_inner AS (

  SELECT 
    in0.* EXCEPT (`MacroRecordID`),
    in1.*
  
  FROM Summarize_175_3124 AS in0
  INNER JOIN Join_153_3124_inner AS in1
     ON (in0.MacroRecordID = in1.MacroRecordID)

),

Union_177_3124_reformat_0 AS (

  SELECT 
    ACV AS ACV,
    `Actual Closed Date` AS `Actual Closed Date`,
    ContractTermDays AS ContractTermDays,
    `Created Date` AS `Created Date`,
    `Customer Data ARR` AS `Customer Data ARR`,
    EndDate_Annualization AS EndDate_Annualization,
    Engine_ContractDays AS Engine_ContractDays,
    `Expected Renewal Date` AS `Expected Renewal Date`,
    F9 AS F9,
    ManualRecordID AS ManualRecordID,
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    Notes AS Notes,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    `Order: Order` AS `Order: Order`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Start Date` AS `Order: Start Date`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    Origin AS Origin,
    `PreReductions_Total Price` AS `PreReductions_Total Price`,
    Product AS Product,
    `Product Code` AS `Product Code`,
    Quantity AS Quantity,
    RecordID AS RecordID,
    Stage AS Stage,
    StartDate_Annualization AS StartDate_Annualization,
    `Sum_Total Price (new)` AS `Sum_Total Price (new)`,
    TCV AS TCV,
    TS_ContractDays AS TS_ContractDays,
    prophecy_recordId_564 AS prophecy_recordId_564
  
  FROM Join_176_3124_inner AS in0

),

Union_177_3124 AS (

  SELECT * 
  
  FROM Union_177_3124_reformat_0 AS in0
  
  UNION ALL
  
  SELECT * 
  
  FROM Union_177_3124_reformat_1 AS in1

),

Filter_165_3124 AS (

  SELECT * 
  
  FROM Union_177_3124 AS in0
  
  WHERE (
          (NOT((`Mas90 Customer Number` IS NULL) OR ((LENGTH(`Mas90 Customer Number`)) = 0)))
          AND (
                (
                  NOT(
                    UPPER(`Mas90 Customer Number`) = UPPER('REMOVE'))
                )
                OR (`Mas90 Customer Number` IS NULL)
              )
        )

)

SELECT *

FROM Filter_165_3124
