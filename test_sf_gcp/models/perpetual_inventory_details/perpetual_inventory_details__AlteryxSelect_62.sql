{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_27_0 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory_details__Formula_27_0')}}

),

Directory_57 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'Directory_57') }}

),

Directory_57_reformat AS (

  SELECT 
    PATH AS FULLPATH,
    PARENT_DIRECTORY AS DIRECTORY,
    NAME AS FILENAME,
    NAME AS SHORTFILENAME,
    CREATION_TIME AS CREATIONTIME,
    MODIFICATION_TIME AS LASTACCESSTIME,
    MODIFICATION_TIME AS LASTWRITETIME,
    SIZE_IN_BYTES AS SIZE,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTEARCHIVE,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTECOMPRESSED,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTEENCRYPTED,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTEHIDDEN,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTEREADONLY,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTESYSTEM,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTETEMPORARY
  
  FROM Directory_57 AS in0

),

AlteryxSelect_59 AS (

  SELECT FILENAME AS FILENAME
  
  FROM Directory_57_reformat AS in0

),

Formula_58_0 AS (

  SELECT 
    (TO_CHAR((REGEXP_REPLACE(FILENAME, '.yxdb', '')), 'YYYY-MM-DD')) AS FILEDATE,
    *
  
  FROM AlteryxSelect_59 AS in0

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
  
  WHERE ((FILEDATE >= STARTDATE) AND (FILEDATE < ENDDATE))

),

AlteryxSelect_62 AS (

  SELECT FILEDATE AS FILEDATE
  
  FROM Filter_61 AS in0

)

SELECT *

FROM AlteryxSelect_62
