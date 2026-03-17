{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_69_postRename AS (

  SELECT *
  
  FROM {{ ref('DRP_VQF_Concat__Union_69_postRename')}}

),

FieldInfo_13 AS (

  {{ SchemaInfo('Union_69_postRename') }}

),

Cleanse_14 AS (

  {{
    prophecy_basics.DataCleansing(
      ['FieldInfo_13'], 
      [
        { "name": "Name", "dataType": "String" }, 
        { "name": "variableType", "dataType": "String" }, 
        { "name": "Size", "dataType": "Integer" }, 
        { "name": "Description", "dataType": "String" }, 
        { "name": "Scale", "dataType": "Integer" }, 
        { "name": "Source", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['Name'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      false, 
      false, 
      true, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Formula_15_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Name, 'ddMmmyyyy', '')) AS string) AS Name,
    * EXCEPT (`name`)
  
  FROM Cleanse_14 AS in0

),

Cleanse_16 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_15_0'], 
      [
        { "name": "Name", "dataType": "String" }, 
        { "name": "variableType", "dataType": "String" }, 
        { "name": "Size", "dataType": "Integer" }, 
        { "name": "Description", "dataType": "String" }, 
        { "name": "Scale", "dataType": "Integer" }, 
        { "name": "Source", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['Name', 'variableType', 'Size', 'Scale', 'Source', 'Description'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

)

SELECT *

FROM Cleanse_16
