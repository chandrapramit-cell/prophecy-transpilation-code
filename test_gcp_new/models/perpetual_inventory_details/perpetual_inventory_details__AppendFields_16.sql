{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Directory_11 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'Directory_11') }}

),

Directory_11_reformat AS (

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
  
  FROM Directory_11 AS in0

),

Filter_15 AS (

  SELECT * 
  
  FROM Directory_11_reformat AS in0
  
  WHERE (FileName > '2016-05-28.yxdb')

),

AlteryxSelect_12 AS (

  SELECT FileName AS FileName
  
  FROM Filter_15 AS in0

),

Formula_13_0 AS (

  SELECT 
    (TO_DATE((REGEXP_REPLACE(FileName, '.yxdb', '')), 'yyyy-MM-dd')) AS FileDate,
    *
  
  FROM AlteryxSelect_12 AS in0

),

Summarize_14 AS (

  SELECT 
    MIN(FileDate) AS FirstDate,
    MAX(FileDate) AS LastDate
  
  FROM Formula_13_0 AS in0

),

Formula_27_0 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory_details__Formula_27_0')}}

),

AppendFields_16 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Formula_27_0 AS in0
  INNER JOIN Summarize_14 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_16
