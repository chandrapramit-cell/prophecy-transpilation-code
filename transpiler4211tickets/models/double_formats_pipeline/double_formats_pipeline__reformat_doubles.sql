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

  {#Cleans and standardizes numeric fields stored as text so revenue and metric calculations are accurate and reliable, converting misformatted numbers into usable values and defaulting invalid entries to zero to avoid reporting errors.#}
  SELECT 
    id,
    double_value,
    -- Clean the double_value by removing commas and cast to double
    CAST(REPLACE(double_value, ',', '') AS DOUBLE) AS cleaned_double,
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
