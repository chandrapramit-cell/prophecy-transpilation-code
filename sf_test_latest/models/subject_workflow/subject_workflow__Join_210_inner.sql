{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_208_0 AS (

  SELECT *
  
  FROM {{ ref('subject_workflow__Formula_208_0')}}

),

AlteryxSelect_213 AS (

  SELECT *
  
  FROM {{ ref('subject_workflow__AlteryxSelect_213')}}

),

Join_210_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("STUDY_ID", "SUBJECT_ID", "SRC_SYS_NAME")
  
  FROM Formula_208_0 AS in0
  INNER JOIN AlteryxSelect_213 AS in1
     ON ((in0.SUBJECT_ID = in1.SUBJECT_ID) AND (in0.STUDY_ID = in1.STUDY_ID))

)

SELECT *

FROM Join_210_inner
