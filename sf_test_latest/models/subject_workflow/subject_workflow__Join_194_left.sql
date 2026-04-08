{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Join_288_left_UnionFullOuter AS (

  SELECT *
  
  FROM {{ ref('subject_workflow__Join_288_left_UnionFullOuter')}}

),

DynamicRename_351 AS (

  SELECT *
  
  FROM {{ ref('subject_workflow__DynamicRename_351')}}

),

Join_194_left AS (

  SELECT in0.*
  
  FROM Join_288_left_UnionFullOuter AS in0
  LEFT JOIN DynamicRename_351 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

)

SELECT *

FROM Join_194_left
