{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Directory_57 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'Directory_57') }}

),

Directory_57_reformat AS (

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
  
  FROM Directory_57 AS in0

),

AlteryxSelect_59 AS (

  SELECT FileName AS FileName
  
  FROM Directory_57_reformat AS in0

),

Formula_58_0 AS (

  SELECT 
    (TO_DATE((REGEXP_REPLACE(FileName, '.yxdb', '')), 'yyyy-MM-dd')) AS FileDate,
    *
  
  FROM AlteryxSelect_59 AS in0

),

Formula_27_0 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory_details__Formula_27_0')}}

),

AppendFields_60 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Formula_27_0 AS in0
  INNER JOIN Formula_58_0 AS in1
     ON TRUE

),

Filter_61 AS (

  SELECT * 
  
  FROM AppendFields_60 AS in0
  
  WHERE ((FileDate >= StartDate) AND (FileDate < EndDate))

),

AlteryxSelect_62 AS (

  SELECT FileDate AS FileDate
  
  FROM Filter_61 AS in0

)

SELECT *

FROM AlteryxSelect_62
