{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_33 AS (

  SELECT *
  
  FROM {{ ref('DRP_VQF_Concat__AlteryxSelect_33')}}

),

FieldInfo_18 AS (

  {{ SchemaInfo('AlteryxSelect_33') }}

),

Cleanse_19 AS (

  {{
    prophecy_basics.DataCleansing(
      ['FieldInfo_18'], 
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

Formula_20_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Name, 'ddMmmyyyy', '')) AS string) AS Name,
    * EXCEPT (`name`)
  
  FROM Cleanse_19 AS in0

),

Formula_20_1 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Name, '', ' ')) AS string) AS Name,
    * EXCEPT (`name`)
  
  FROM Formula_20_0 AS in0

),

Cleanse_21 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_20_1'], 
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

FROM Cleanse_21
