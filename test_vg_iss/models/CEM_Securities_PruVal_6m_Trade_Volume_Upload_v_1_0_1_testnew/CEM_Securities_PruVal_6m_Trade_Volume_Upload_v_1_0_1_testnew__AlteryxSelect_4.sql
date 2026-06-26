{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Directory_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew', 'Directory_1') }}

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

TextToColumns_2 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Directory_1_reformat'], 
      'FileName', 
      "\[\.\]", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'FileName', 
      'FileName', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_2_dropGem_0 AS (

  SELECT 
    FileName_1_FileName AS FileName1,
    FileName_2_FileName AS FileName2,
    * EXCEPT (`FileName_1_FileName`, `FileName_2_FileName`)
  
  FROM TextToColumns_2 AS in0

),

Formula_3_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE((CONCAT(FileName1, '.csv')), '^[NaN_]+|[NaN_]+$', '')) AS string) AS FileName1,
    * EXCEPT (`filename1`)
  
  FROM TextToColumns_2_dropGem_0 AS in0

),

Formula_3_1 AS (

  SELECT 
    CAST((CONCAT(FullPath, '|||', FileName1)) AS string) AS FullPath,
    * EXCEPT (`fullpath`)
  
  FROM Formula_3_0 AS in0

),

AlteryxSelect_4 AS (

  SELECT FullPath AS FullPath
  
  FROM Formula_3_1 AS in0

)

SELECT *

FROM AlteryxSelect_4
