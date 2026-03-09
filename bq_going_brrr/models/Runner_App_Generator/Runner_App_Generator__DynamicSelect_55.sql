{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Directory_56 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Runner_App_Generator', 'Directory_56') }}

),

Directory_56_reformat AS (

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
  
  FROM Directory_56 AS in0

),

Filter_59 AS (

  SELECT * 
  
  FROM Directory_56_reformat AS in0
  
  WHERE ((LENGTH((REGEXP_EXTRACT(FullPath, '^.+(?:YX|yx)(?:MC|mc|MD|md|WZ|wz)$', 0)))) > 0)

),

RecordID_58 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `AYX_RecordID`
  
  FROM Filter_59

),

AlteryxSelect_57 AS (

  SELECT 
    LastWriteTime AS LastWriteTime,
    CreationTime AS CreationTime,
    AttributeCompressed AS AttributeCompressed,
    Directory AS Directory,
    FileName AS FileName,
    ShortFileName AS ShortFileName,
    AttributeHidden AS AttributeHidden,
    AttributeEncrypted AS AttributeEncrypted,
    Size AS Size,
    AttributeTemporary AS AttributeTemporary,
    AttributeReadOnly AS AttributeReadOnly,
    AYX_RecordID AS AYX_RecordID,
    AttributeArchive AS AttributeArchive,
    LastAccessTime AS LastAccessTime,
    AttributeSystem AS AttributeSystem,
    FullPath AS WorkflowFullPath
  
  FROM RecordID_58 AS in0

),

DynamicSelect_55 AS (

  {{ prophecy_basics.ToDo('Dynamic Select tool is not supported in sql.') }}

)

SELECT *

FROM DynamicSelect_55
