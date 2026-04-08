{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH all_samples_yxd_11 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('TS_Factory_Sample1', 'all_samples_yxd_11') }}

),

Detour_67_12_out0 AS (

  {#VisualGroup: ToolsUsed#}
  SELECT * 
  
  FROM all_samples_yxd_11 AS in0
  
  WHERE FALSE

),

RecordID_69_12 AS (

  {#VisualGroup: ToolsUsed#}
  {{
    prophecy_basics.RecordID(
      ['Detour_67_12_out0'], 
      'incremental_id', 
      'RECORDID', 
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

  {#VisualGroup: ToolsUsed#}
  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

DynamicSelect_75_12 AS (

  {#VisualGroup: ToolsUsed#}
  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

DynamicSelect_71_12 AS (

  {#VisualGroup: ToolsUsed#}
  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

JoinMultiple_76_12 AS (

  {#VisualGroup: ToolsUsed#}
  SELECT 
    in0.RECORDID AS RECORDID,
    in1.RECORDID AS INPUT_HASH2_RECORDID,
    in2.RECORDID AS INPUT_HASH3_RECORDID
  
  FROM DynamicSelect_70_12 AS in0
  FULL JOIN DynamicSelect_75_12 AS in1
     ON (in0.RECORDID = in1.RECORDID)
  FULL JOIN DynamicSelect_71_12 AS in2
     ON (coalesce(in0.RECORDID, in1.RECORDID) = in2.RECORDID)

),

DynamicSelect_77_12 AS (

  {#VisualGroup: ModuleDescription#}
  SELECT 
    UNITS AS UNITS,
    PRODUCT AS PRODUCT,
    "NUMBER OF NEW STORES" AS "NUMBER OF NEW STORES",
    "TOTAL SALES AMOUNT IN DOLLARS" AS "TOTAL SALES AMOUNT IN DOLLARS"
  
  FROM JoinMultiple_76_12 AS in0

),

Detour_67_12_out1 AS (

  {#VisualGroup: ToolsUsed#}
  SELECT * 
  
  FROM all_samples_yxd_11 AS in0
  
  WHERE TRUE

),

RecordID_74_12 AS (

  {#VisualGroup: ToolsUsed#}
  {{
    prophecy_basics.RecordID(
      ['Detour_67_12_out1'], 
      'incremental_id', 
      'RECORDID', 
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

  {#VisualGroup: ModuleDescription#}
  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

DynamicSelect_68_12 AS (

  {#VisualGroup: ToolsUsed#}
  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

),

Join_73_12_inner AS (

  {#VisualGroup: ToolsUsed#}
  SELECT 
    in1.RECORDID AS RIGHT_RECORDID,
    in0.*,
    in1.* EXCLUDE ("RECORDID")
  
  FROM DynamicSelect_68_12 AS in0
  INNER JOIN DynamicSelect_72_12 AS in1
     ON (in0.RECORDID = in1.RECORDID)

),

DynamicSelect_78_12 AS (

  {#VisualGroup: ToolsUsed#}
  SELECT 
    UNITS AS UNITS,
    PRODUCT AS PRODUCT
  
  FROM Join_73_12_inner AS in0

),

DetourEnd_79_12 AS (

  {#VisualGroup: InterfaceTools#}
  {{
    prophecy_basics.UnionByName(
      ['DynamicSelect_77_12', 'DynamicSelect_78_12'], 
      [
        '[{"name": "UNITS", "dataType": "Float"}, {"name": "PRODUCT", "dataType": "String"}, {"name": "NUMBER OF NEW STORES", "dataType": "Number"}, {"name": "TOTAL SALES AMOUNT IN DOLLARS", "dataType": "Float"}]', 
        '[{"name": "UNITS", "dataType": "Float"}, {"name": "PRODUCT", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM DetourEnd_79_12
