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
      '[{"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]', 
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

  SELECT 
    to_date(
      concat(
        regexp_replace(
          regexp_replace(format_number(CAST(year(StaticHistoryMonth) AS DOUBLE), 0), ',', '__THS__'), 
          '__THS__', 
          ''), 
        '-12-31')) AS StaticHistoryYearEnd,
    *
  
  FROM Sample_1973 AS in0

)

SELECT *

FROM Formula_1972_0
