{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH unknown_format_requires_schema_validation AS (

  {#Overwrites the seed table 's2' to refresh baseline data used by downstream reports and processes.#}
  SELECT * 
  
  FROM {{ ref('s2')}}

),

sanitized_name AS (

  {#Standardizes names by replacing special characters and spaces with a consistent separator to ensure clean, uniform name fields for reporting, matching, and integration across systems.#}
  SELECT 
    (REGEXP_REPLACE(name, '[\\s!@#$%^&*(),.?":{}|<>\\[\\]=;/\\-+]', '_')) AS name2,
    *
  
  FROM unknown_format_requires_schema_validation AS in0

)

SELECT *

FROM sanitized_name
