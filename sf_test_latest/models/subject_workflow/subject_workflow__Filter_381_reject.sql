{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH JoinMultiple_15 AS (

  SELECT *
  
  FROM {{ ref('subject_workflow__JoinMultiple_15')}}

),

Filter_381_reject AS (

  SELECT * 
  
  FROM JoinMultiple_15 AS in0
  
  WHERE (
          (NOT((STUDY_ID IS NULL) AND (SUBJECT_ID IS NULL)))
          OR (((STUDY_ID IS NULL) AND (SUBJECT_ID IS NULL)) IS NULL)
        )

)

SELECT *

FROM Filter_381_reject
