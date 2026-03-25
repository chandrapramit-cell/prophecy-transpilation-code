{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Directory_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_', 'Directory_1') }}

),

Directory_1_reformat AS (

  SELECT 
    PATH AS FullPath,
    PARENT_DIRECTORY AS Directory,
    NAME AS FileName,
    NAME AS ShortFileName,
    CREATION_TIME AS CreationTime,
    MODIFICATION_TIME AS LastAccessTime,
    MODIFICATION_TIME AS LastWriteTime,
    SIZE_IN_BYTES AS Size,
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
      [{ 'columnName': 'CC_Check', 'dataType': 'String', 'rgxExpression': '_((?:AU|NZ)\\d{2})_' }], 
      '[{"name": "AttributeReadOnly", "dataType": "Boolean"}, {"name": "AttributeEncrypted", "dataType": "Boolean"}, {"name": "Directory", "dataType": "String"}, {"name": "LastWriteTime", "dataType": "Timestamp"}, {"name": "AttributeArchive", "dataType": "Boolean"}, {"name": "AttributeTemporary", "dataType": "Boolean"}, {"name": "Size", "dataType": "Integer"}, {"name": "AttributeHidden", "dataType": "Boolean"}, {"name": "FullPath", "dataType": "String"}, {"name": "LastAccessTime", "dataType": "Timestamp"}, {"name": "FileName", "dataType": "String"}, {"name": "ShortFileName", "dataType": "String"}, {"name": "AttributeCompressed", "dataType": "Boolean"}, {"name": "CreationTime", "dataType": "Timestamp"}, {"name": "AttributeSystem", "dataType": "Boolean"}]', 
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
