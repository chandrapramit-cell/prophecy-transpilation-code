{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Unique_1094 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Unique_1094')}}

),

Formula_708_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_708_0')}}

),

AlteryxSelect_1073 AS (

  SELECT * EXCEPT (`Updated Term`)
  
  FROM Formula_708_0 AS in0

),

table_3124_Exit_macro_op AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_3124_Exit_macro_op') }}

),

Summarize_1072 AS (

  SELECT 
    SUM(TCV) AS TCV,
    SUM(Quantity) AS Quantity,
    `Actual Closed Date` AS `Actual Closed Date`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Order` AS `Order: Order`,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    `Created Date` AS `Created Date`,
    StartDate_Annualization AS StartDate_Annualization,
    Origin AS Origin,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Expected Renewal Date` AS `Expected Renewal Date`,
    `Product Code` AS `Product Code`,
    Product AS Product,
    Stage AS Stage,
    EndDate_Annualization AS EndDate_Annualization,
    `Order: Start Date` AS `Order: Start Date`
  
  FROM table_3124_Exit_macro_op AS in0
  
  GROUP BY 
    `Actual Closed Date`, 
    `Order: Sales Order Number`, 
    `Order: Order`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    `Created Date`, 
    StartDate_Annualization, 
    Origin, 
    `Order: End Date (Calculated)`, 
    `Mas90 Customer Number`, 
    `Order: Activated Date`, 
    `Expected Renewal Date`, 
    `Product Code`, 
    Product, 
    Stage, 
    EndDate_Annualization, 
    `Order: Start Date`

),

Union_709 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_1072', 'AlteryxSelect_1073'], 
      [
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "TCV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_1065 AS (

  SELECT 
    `Mas90 Customer Number` AS CustomerName,
    * EXCEPT (`Mas90 Customer Number`)
  
  FROM Union_709 AS in0

),

Join_1056_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Mas90 Customer Number`)
  
  FROM AlteryxSelect_1065 AS in0
  LEFT JOIN Unique_1094 AS in1
     ON (in0.CustomerName = in1.`Mas90 Customer Number`)

),

RecordID_3074 AS (

  {{
    prophecy_basics.RecordID(
      ['Join_1056_left_UnionLeftOuter'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

MultiFieldFormula_1895 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['RecordID_3074'], 
      "CASE WHEN (isnull(column_value) OR (length(column_value) = 0)) THEN 'N/A' ELSE column_value END", 
      [
        'Quantity', 
        'Actual Closed Date', 
        'Order: Sales Order Number', 
        'Sector', 
        'Territory Name', 
        'Order: Order', 
        'CustomerName', 
        'variableType', 
        'Order: Opportunity: Renewed Contract: Order: Order', 
        'Created Date', 
        'StartDate_Annualization', 
        'Origin', 
        'Order: End Date (Calculated)', 
        'Order: Activated Date', 
        'TCV', 
        'Expected Renewal Date', 
        'Product Code', 
        'Account Owner', 
        'Engine_ContractDays', 
        'Product', 
        'State', 
        'RecordID', 
        'Stage', 
        'Partner Success Owner', 
        'EndDate_Annualization', 
        'Order: Start Date'
      ], 
      ['Sector', 'variableType', 'Territory Name', 'State', 'Account Owner', 'Partner Success Owner'], 
      false, 
      'Suffix', 
      ''
    )
  }}

)

SELECT *

FROM MultiFieldFormula_1895
