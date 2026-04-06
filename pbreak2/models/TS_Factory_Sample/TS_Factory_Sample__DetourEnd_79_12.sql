{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH all_samples_yxd_11 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('TS_Factory_Sample', 'all_samples_yxd_11') }}

),

Detour_67_12_out0 AS (

  SELECT * 
  
  FROM all_samples_yxd_11 AS in0
  
  WHERE FALSE

),

RecordID_69_12 AS (

  {{
    prophecy_basics.RecordID(
      ['Detour_67_12_out0'], 
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

DynamicSelect_70_12 AS (

  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

DynamicSelect_75_12 AS (

  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

DynamicSelect_71_12 AS (

  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

JoinMultiple_76_12 AS (

  SELECT 
    in0.Bookings AS Bookings,
    in0.RecordID AS RecordID,
    in2.Website_hits AS Website_hits,
    in1.RecordID AS Input_hash2_RecordID,
    in2.RecordID AS Input_hash3_RecordID
  
  FROM DynamicSelect_70_12 AS in0
  FULL JOIN DynamicSelect_75_12 AS in1
     ON (in0.RecordID = in1.RecordID)
  FULL JOIN DynamicSelect_71_12 AS in2
     ON (coalesce(in0.RecordID, in1.RecordID) = in2.RecordID)

),

DynamicSelect_77_12 AS (

  SELECT 
    UNITS AS Units,
    PRODUCT AS Product,
    `Number of New Stores` AS `Number of New Stores`,
    `Total Sales Amount in Dollars` AS `Total Sales Amount in Dollars`
  
  FROM JoinMultiple_76_12 AS in0

),

Detour_67_12_out1 AS (

  SELECT * 
  
  FROM all_samples_yxd_11 AS in0
  
  WHERE TRUE

),

RecordID_74_12 AS (

  {{
    prophecy_basics.RecordID(
      ['Detour_67_12_out1'], 
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

DynamicSelect_72_12 AS (

  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

DynamicSelect_68_12 AS (

  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

Join_73_12_inner AS (

  SELECT 
    in1.RecordID AS Right_RecordID,
    in0.*,
    in1.* EXCEPT (`RecordID`)
  
  FROM DynamicSelect_68_12 AS in0
  INNER JOIN DynamicSelect_72_12 AS in1
     ON (in0.RecordID = in1.RecordID)

),

DynamicSelect_78_12 AS (

  SELECT 
    UNITS AS Units,
    PRODUCT AS Product
  
  FROM Join_73_12_inner AS in0

),

DetourEnd_79_12 AS (

  {{
    prophecy_basics.UnionByName(
      ['DynamicSelect_77_12', 'DynamicSelect_78_12'], 
      [
        '[{"name": "Units", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Number of New Stores", "dataType": "Integer"}, {"name": "Total Sales Amount in Dollars", "dataType": "Double"}]', 
        '[{"name": "Units", "dataType": "Double"}, {"name": "Product", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM DetourEnd_79_12
