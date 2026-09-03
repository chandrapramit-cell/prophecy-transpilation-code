{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH DbFileInput_323_3230 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DbFileInput_323_3230'
    )
  }}

),

HistoricalTadpo_3330 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'HistoricalTadpo_3330'
    )
  }}

),

Union_3329 AS (

  {{
    prophecy_basics.UnionByName(
      ['DbFileInput_323_3230', 'HistoricalTadpo_3330'], 
      [
        '[{"name": "F16", "dataType": "String"}, {"name": "F12", "dataType": "String"}, {"name": "F9", "dataType": "String"}, {"name": "F4", "dataType": "String"}, {"name": "F20", "dataType": "String"}, {"name": "F10", "dataType": "String"}, {"name": "F13", "dataType": "String"}, {"name": "F19", "dataType": "String"}, {"name": "F3", "dataType": "String"}, {"name": "F7", "dataType": "String"}, {"name": "F2", "dataType": "String"}, {"name": "F22", "dataType": "String"}, {"name": "F11", "dataType": "String"}, {"name": "F15", "dataType": "String"}, {"name": "F18", "dataType": "String"}, {"name": "F6", "dataType": "String"}, {"name": "F1", "dataType": "String"}, {"name": "F21", "dataType": "String"}, {"name": "F17", "dataType": "String"}, {"name": "F8", "dataType": "String"}, {"name": "F14", "dataType": "String"}, {"name": "F5", "dataType": "String"}]', 
        '[{"name": "F16", "dataType": "String"}, {"name": "F12", "dataType": "String"}, {"name": "F9", "dataType": "String"}, {"name": "F4", "dataType": "String"}, {"name": "F20", "dataType": "String"}, {"name": "F10", "dataType": "String"}, {"name": "F13", "dataType": "String"}, {"name": "F19", "dataType": "String"}, {"name": "F3", "dataType": "String"}, {"name": "F7", "dataType": "String"}, {"name": "F2", "dataType": "String"}, {"name": "F11", "dataType": "String"}, {"name": "F15", "dataType": "String"}, {"name": "F18", "dataType": "String"}, {"name": "F6", "dataType": "String"}, {"name": "F1", "dataType": "String"}, {"name": "F21", "dataType": "String"}, {"name": "F17", "dataType": "String"}, {"name": "F8", "dataType": "String"}, {"name": "F14", "dataType": "String"}, {"name": "F5", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_3231 AS (

  SELECT * 
  
  FROM Union_3329 AS in0
  
  WHERE (NOT(F1 IS NULL))

),

DynamicRename_3232 AS (

  SELECT 
    F1 AS `QB Date`,
    F2 AS `Transaction Type`,
    F3 AS Num,
    F4 AS `Customer Name`,
    F5 AS `Memo/Description`,
    F6 AS Split,
    F7 AS Amount,
    F8 AS Stripe,
    F9 AS `Center Name`,
    F10 AS `SF Date`,
    F11 AS `Customer ID`,
    F12 AS Customer,
    F13 AS `Sale Date`,
    F14 AS `Start Date`,
    F15 AS `Expiration Date`,
    F16 AS `Order #`,
    F17 AS `SF Rev`,
    F18 AS variableDate,
    F19 AS Mas90,
    F20 AS `Customer Name2`,
    F21 AS Revenue,
    F22 AS Column1
  
  FROM Filter_3231 AS in0

),

DynamicRename_3232_row_number AS (

  {{
    prophecy_basics.RecordID(
      ['DynamicRename_3232'], 
      'incremental_id', 
      'prophecy_row_id', 
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

DynamicRename_3232_filter AS (

  SELECT * 
  
  FROM DynamicRename_3232_row_number AS in0
  
  WHERE (
          (
            NOT(
              prophecy_row_id = 1)
          ) OR (prophecy_row_id IS NULL)
        )

),

DynamicRename_3232_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM DynamicRename_3232_filter AS in0

),

AlteryxSelect_3233 AS (

  SELECT * EXCEPT (`Transaction Type`, 
         `Num`, 
         `Customer Name`, 
         `Memo/Description`, 
         `Split`, 
         `Amount`, 
         `Stripe`, 
         `Center Name`, 
         `SF Date`, 
         `Customer ID`, 
         `Customer`, 
         `Sale Date`, 
         `Start Date`, 
         `Expiration Date`, 
         `Order #`, 
         `SF Rev`, 
         `Column1`)
  
  FROM DynamicRename_3232_drop_0 AS in0

)

SELECT *

FROM AlteryxSelect_3233
