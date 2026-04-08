{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_20 AS (

  SELECT *
  
  FROM {{ ref('subject_workflow__AlteryxSelect_20')}}

),

Filter_23 AS (

  SELECT * 
  
  FROM AlteryxSelect_20 AS in0
  
  WHERE (STANDARD_VISIT_NAME = 'Randomization')

),

DynamicRename_24 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['Filter_23'], 
      ['IRT_DT', 'EDC_TABLE', 'EDC_DT', 'IRT_TABLE'], 
      'editPrefixSuffix', 
      ['STUDY_ID', 'SUBJECT_ID', 'STANDARD_VISIT_NAME', 'EDC_DT', 'IRT_DT', 'EDC_TABLE', 'IRT_TABLE'], 
      'Prefix', 
      'randomization_', 
      ""
    )
  }}

),

AlteryxSelect_26 AS (

  SELECT * EXCLUDE ("STANDARD_VISIT_NAME")
  
  FROM DynamicRename_24 AS in0

)

SELECT *

FROM AlteryxSelect_26
