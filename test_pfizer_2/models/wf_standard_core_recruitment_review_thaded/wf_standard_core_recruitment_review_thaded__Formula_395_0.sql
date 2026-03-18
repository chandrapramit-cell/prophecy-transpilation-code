{{
  config({    
    "materialized": "ephemeral",
    "database": "rohit",
    "schema": "default"
  })
}}

WITH Formula_374_1 AS (

  SELECT *
  
  FROM {{ ref('wf_standard_core_recruitment_review_thaded__Formula_374_1')}}

),

Formula_395_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (activation_plan_to_date_src = 'Unavailable')
          THEN 'Plan Unavailable'
        WHEN (sites100_pct_cmp = 100)
          THEN 'Activation Complete'
        WHEN (
          (CAST(activation_plan_to_date AS DOUBLE) IN (CAST(0 AS DOUBLE)))
          AND (CAST(activation_actuals AS DOUBLE) IN (CAST(0 AS DOUBLE)))
        )
          THEN 'Not Started'
        WHEN (ABS(activation_to_date_fraction) >= 0.05)
          THEN (
            CASE
              WHEN (activation_to_date_fraction < 0)
                THEN 'Ahead'
              ELSE 'Behind'
            END
          )
        ELSE 'On Plan'
      END
    ) AS string) AS activation_indicator,
    *
  
  FROM Formula_374_1 AS in0

)

SELECT *

FROM Formula_395_0
