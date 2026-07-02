{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Directory_46 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume', 'Directory_46') }}

),

Directory_46_reformat AS (

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
  
  FROM Directory_46 AS in0

),

AlteryxSelect_47 AS (

  SELECT FullPath AS FullPath
  
  FROM Directory_46_reformat AS in0

)

SELECT *

FROM AlteryxSelect_47
