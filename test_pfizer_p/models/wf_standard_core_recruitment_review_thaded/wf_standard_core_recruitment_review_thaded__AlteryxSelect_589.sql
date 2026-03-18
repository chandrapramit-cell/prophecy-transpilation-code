{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_395_0 AS (

  SELECT *
  
  FROM {{ ref('wf_standard_core_recruitment_review_thaded__Formula_395_0')}}

),

AlteryxSelect_589 AS (

  SELECT 
    study_id AS study_id,
    activation_indicator AS activation_indicator
  
  FROM Formula_395_0 AS in0

)

SELECT *

FROM AlteryxSelect_589
