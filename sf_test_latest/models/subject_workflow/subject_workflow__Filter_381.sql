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

Filter_381 AS (

  SELECT * 
  
  FROM JoinMultiple_15 AS in0
  
  WHERE ((STUDY_ID IS NULL) AND (SUBJECT_ID IS NULL))

)

SELECT *

FROM Filter_381
