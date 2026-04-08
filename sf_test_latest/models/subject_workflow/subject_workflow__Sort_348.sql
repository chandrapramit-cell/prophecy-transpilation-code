{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Join_194_left AS (

  SELECT *
  
  FROM {{ ref('subject_workflow__Join_194_left')}}

),

Summarize_215 AS (

  SELECT 
    COUNT(DISTINCT SUBJECT_ID) AS COUNTDISTINCT_SUBJECT_ID,
    STUDY_ID AS STUDY_ID
  
  FROM Join_194_left AS in0
  
  GROUP BY STUDY_ID

),

Join_347_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("STUDY_ID")
  
  FROM Join_194_left AS in0
  INNER JOIN Summarize_215 AS in1
     ON (in0.STUDY_ID = in1.STUDY_ID)

),

Sort_348 AS (

  SELECT * 
  
  FROM Join_347_inner AS in0
  
  ORDER BY COUNTDISTINCT_SUBJECT_ID DESC, STUDY_ID ASC

)

SELECT *

FROM Sort_348
