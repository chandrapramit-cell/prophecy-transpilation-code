{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_212 AS (

  SELECT * 
  
  FROM {{ ref('seed_212')}}

),

TextInput_212_cast AS (

  SELECT 
    CAST(STUDY_ID AS STRING) AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    CAST(SRC_SYS_NAME AS STRING) AS SRC_SYS_NAME
  
  FROM TextInput_212 AS in0

),

AlteryxSelect_213 AS (

  SELECT 
    CAST(STUDY_ID AS STRING) AS STUDY_ID,
    CAST(SUBJECT_ID AS STRING) AS SUBJECT_ID,
    CAST(SRC_SYS_NAME AS STRING) AS SRC_SYS_NAME
  
  FROM TextInput_212_cast AS in0

)

SELECT *

FROM AlteryxSelect_213
