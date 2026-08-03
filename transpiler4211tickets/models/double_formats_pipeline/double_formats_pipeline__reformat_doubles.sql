{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH double_formats_seed AS (

  {#Provides a seeded double_formats table as a stable reference for downstream transformations, validations, and reporting.#}
  SELECT * 
  
  FROM {{ ref('double_formats')}}

),

reformat_doubles AS (

  {#Converts messy numeric strings into a standardized numeric value (defaults to 0 when non-numeric), enabling consistent financial calculations and reporting.#}
  SELECT 
    id,
    double_value,
    -- non-numeric → 0                                                                                                                                                   
    coalesce(
      -- always DOUBLE                                                                                                                                                     
      TRY_CAST(REGEXP_EXTRACT(
        REGEXP_REPLACE(double_value, '[ ,\\x27]', ''), 
        -- strip space, comma, apostrophe                                                                                                                                     
        '^[0-9]*\\.?[0-9]+', 
        -- leading number, one optional '.'                                                                                                                                  
        0) AS DOUBLE), 
      0) AS doubled_value
  
  FROM double_formats_seed

)

SELECT *

FROM reformat_doubles
