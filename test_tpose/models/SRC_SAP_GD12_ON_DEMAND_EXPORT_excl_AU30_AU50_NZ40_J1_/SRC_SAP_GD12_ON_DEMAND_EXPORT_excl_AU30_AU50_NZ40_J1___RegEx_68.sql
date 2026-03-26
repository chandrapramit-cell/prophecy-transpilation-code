{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Directory_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_', 'Directory_1') }}

),

Directory_1_reformat AS (

  {#VisualGroup: J1directory#}
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

  {#VisualGroup: J1directory#}
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
