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

reformat_as_decimal AS (

  SELECT 
    id,
    big_decimal_value,
    CAST(big_decimal_value AS DECIMAL (38, 10)) AS big_decimal_as_decimal_38_10
  
  FROM big_decimal_strings

)

SELECT *

FROM reformat_as_decimal
