{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Directory_11 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory_details', 'Directory_11') }}

),

Directory_11_reformat AS (

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
  
  FROM Directory_11 AS in0

),

Filter_15 AS (

  SELECT * 
  
  FROM Directory_11_reformat AS in0
  
  WHERE (FILENAME > '2016-05-28.yxdb')

),

AlteryxSelect_12 AS (

  SELECT FILENAME AS FILENAME
  
  FROM Filter_15 AS in0

),

Formula_13_0 AS (

  SELECT 
    (TO_CHAR((REGEXP_REPLACE(FILENAME, '.yxdb', '')), 'YYYY-MM-DD')) AS FILEDATE,
    *
  
  FROM AlteryxSelect_12 AS in0

),

Summarize_14 AS (

  SELECT 
    MIN(FILEDATE) AS FIRSTDATE,
    MAX(FILEDATE) AS LASTDATE
  
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
