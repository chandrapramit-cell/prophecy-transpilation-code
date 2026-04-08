{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH aka_GPDIP_EDLUD_369 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('subject_workflow', 'aka_GPDIP_EDLUD_369') }}

),

AlteryxSelect_20 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    STANDARD_VISIT_NAME AS STANDARD_VISIT_NAME,
    EDC_DT AS EDC_DT,
    IRT_DT AS IRT_DT,
    EDC_TABLE AS EDC_TABLE,
    IRT_TABLE AS IRT_TABLE
  
  FROM aka_GPDIP_EDLUD_369 AS in0

)

SELECT *

FROM AlteryxSelect_20
