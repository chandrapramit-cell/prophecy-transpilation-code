{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_1894 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_1894')}}

),

Sample_1973 AS (

  {{
    prophecy_basics.Sample(
      ['AlteryxSelect_1894'], 
      '[{"name": "ContractStartDate", "dataType": "Date"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "CustomerName", "dataType": "String"}, {"name": "TCV", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      1, 
      [{ 'expression': { 'expression': 'RecordID' }, 'sortType': 'asc' }]
    )
  }}

),

Formula_1972_0 AS (

  {#Adds fixed historical year and month alongside all fields from Sample_1973 for archival or reporting purposes.#}
  SELECT 
    '1' AS StaticHistoryYearEnd,
    '1' AS StaticHistoryMonth,
    *
  
  FROM Sample_1973 AS in0

)

SELECT *

FROM Formula_1972_0
