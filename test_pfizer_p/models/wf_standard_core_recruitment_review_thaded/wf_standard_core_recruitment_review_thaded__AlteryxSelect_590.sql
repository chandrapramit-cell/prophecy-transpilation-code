{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_374_1 AS (

  SELECT *
  
  FROM {{ ref('wf_standard_core_recruitment_review_thaded__Formula_374_1')}}

),

AlteryxSelect_590 AS (

  SELECT 
    study_id AS study_id,
    activation_plan AS activation_plan,
    activation_plan_src AS activation_plan_src,
    activation_to_date_variance AS activation_to_date_variance,
    activation_to_date_fraction AS activation_to_date_fraction,
    activation_total_variance AS activation_total_variance
  
  FROM Formula_374_1 AS in0

)

SELECT *

FROM AlteryxSelect_590
