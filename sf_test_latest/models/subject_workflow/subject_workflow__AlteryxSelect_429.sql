{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH aka_GPDIP_EDLUD_428 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('subject_workflow', 'aka_GPDIP_EDLUD_428') }}

),

AlteryxSelect_429 AS (

  SELECT 
    SUBJ_STRATA_NUM AS SBJ_STR_STRATA_NUM,
    CAST(SUBJ_COHORT_NUM AS INTEGER) AS SBJ_COH_COHORT_NUM,
    * EXCLUDE ("SUBJ_STRATA_NUM", "SUBJ_COHORT_NUM")
  
  FROM aka_GPDIP_EDLUD_428 AS in0

)

SELECT *

FROM AlteryxSelect_429
