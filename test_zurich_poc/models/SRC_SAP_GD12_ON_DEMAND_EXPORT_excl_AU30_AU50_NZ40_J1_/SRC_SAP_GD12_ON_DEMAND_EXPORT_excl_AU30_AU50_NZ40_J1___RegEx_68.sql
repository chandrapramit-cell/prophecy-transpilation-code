{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Directory_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_', 'Directory_1') }}

),

Directory_1_reformat AS (

  SELECT 
    path AS FullPath,
    parent_directory AS Directory,
    name AS FileName,
    name AS ShortFileName,
    creation_time AS CreationTime,
    modification_time AS LastAccessTime,
    modification_time AS LastWriteTime,
    size_in_bytes AS Size,
    CAST(NULL AS BOOLEAN) AS AttributeArchive,
    CAST(NULL AS BOOLEAN) AS AttributeCompressed,
    CAST(NULL AS BOOLEAN) AS AttributeEncrypted,
    CAST(NULL AS BOOLEAN) AS AttributeHidden,
    CAST(NULL AS BOOLEAN) AS AttributeReadOnly,
    CAST(NULL AS BOOLEAN) AS AttributeSystem,
    CAST(NULL AS BOOLEAN) AS AttributeTemporary
  
  FROM Directory_1 AS in0

),

RegEx_68 AS (

  {{
    prophecy_basics.Regex(
      ['Directory_1_reformat'], 
      [{ 'columnName': 'CC_Check', 'dataType': 'String', 'rgxExpression': '((?:AU|NZ)\\d{2})' }], 
      '[{"name": "FullPath", "dataType": "String"}, {"name": "Directory", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ShortFileName", "dataType": "String"}, {"name": "CreationTime", "dataType": "Bigint"}, {"name": "LastAccessTime", "dataType": "Bigint"}, {"name": "LastWriteTime", "dataType": "Bigint"}, {"name": "Size", "dataType": "Bigint"}, {"name": "AttributeArchive", "dataType": "Boolean"}, {"name": "AttributeCompressed", "dataType": "Boolean"}, {"name": "AttributeEncrypted", "dataType": "Boolean"}, {"name": "AttributeHidden", "dataType": "Boolean"}, {"name": "AttributeReadOnly", "dataType": "Boolean"}, {"name": "AttributeSystem", "dataType": "Boolean"}, {"name": "AttributeTemporary", "dataType": "Boolean"}]', 
      'FileName', 
      '_((?:AU|NZ)\d{2})_', 
      'parse', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false
    )
  }}

)

SELECT *

FROM RegEx_68
