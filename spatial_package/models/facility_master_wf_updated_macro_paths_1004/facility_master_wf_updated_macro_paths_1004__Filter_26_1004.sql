{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH table_1004_Input43_macro_ip AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_1004_Input43_macro_ip') }}

),

Filter_26_1004 AS (

  SELECT * 
  
  FROM table_1004_Input43_macro_ip AS in0
  
  WHERE {{ var('variable26_Expression') }}

)

SELECT *

FROM Filter_26_1004
