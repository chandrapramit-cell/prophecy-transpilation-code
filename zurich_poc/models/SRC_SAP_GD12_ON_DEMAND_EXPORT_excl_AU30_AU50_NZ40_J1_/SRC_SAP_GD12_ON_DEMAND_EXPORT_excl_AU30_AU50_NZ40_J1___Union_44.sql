{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Directory_43 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_', 'Directory_43') }}

),

TextInput_70_cast AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___TextInput_70_cast')}}

),

RegEx_68 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___RegEx_68')}}

),

Join_71_left AS (

  SELECT in0.*
  
  FROM RegEx_68 AS in0
  ANTI JOIN TextInput_70_cast AS in1
     ON (in0.CC_Check = in1.COMPANY_CODE)

),

Directory_43_reformat AS (

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
  
  FROM Directory_43 AS in0

),

Union_44 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_71_left', 'Directory_43_reformat'], 
      [
        '[{"name": "AttributeReadOnly", "dataType": "Boolean"}, {"name": "AttributeEncrypted", "dataType": "Boolean"}, {"name": "Directory", "dataType": "String"}, {"name": "LastWriteTime", "dataType": "Timestamp"}, {"name": "AttributeArchive", "dataType": "Boolean"}, {"name": "AttributeTemporary", "dataType": "Boolean"}, {"name": "Size", "dataType": "Integer"}, {"name": "AttributeHidden", "dataType": "Boolean"}, {"name": "FullPath", "dataType": "String"}, {"name": "LastAccessTime", "dataType": "Timestamp"}, {"name": "FileName", "dataType": "String"}, {"name": "CC_Check", "dataType": "String"}, {"name": "ShortFileName", "dataType": "String"}, {"name": "AttributeCompressed", "dataType": "Boolean"}, {"name": "CreationTime", "dataType": "Timestamp"}, {"name": "AttributeSystem", "dataType": "Boolean"}]', 
        '[{"name": "AttributeReadOnly", "dataType": "Boolean"}, {"name": "AttributeEncrypted", "dataType": "Boolean"}, {"name": "Directory", "dataType": "String"}, {"name": "LastWriteTime", "dataType": "Timestamp"}, {"name": "AttributeArchive", "dataType": "Boolean"}, {"name": "AttributeTemporary", "dataType": "Boolean"}, {"name": "Size", "dataType": "Integer"}, {"name": "AttributeHidden", "dataType": "Boolean"}, {"name": "FullPath", "dataType": "String"}, {"name": "LastAccessTime", "dataType": "Timestamp"}, {"name": "FileName", "dataType": "String"}, {"name": "ShortFileName", "dataType": "String"}, {"name": "AttributeCompressed", "dataType": "Boolean"}, {"name": "CreationTime", "dataType": "Timestamp"}, {"name": "AttributeSystem", "dataType": "Boolean"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_44
