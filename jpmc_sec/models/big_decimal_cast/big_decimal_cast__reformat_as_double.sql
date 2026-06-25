{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH big_decimal_strings AS (

  SELECT * 
  
  FROM {{ ref('big_decimal_strings')}}

),

reformat_as_double AS (

  SELECT 
    id,
    big_decimal_value,
    CAST(big_decimal_value AS DOUBLE) AS big_decimal_as_double
  
  FROM big_decimal_strings

)

SELECT *

FROM reformat_as_double
