{{
  config({    
    "materialized": "table",
    "alias": "back_surgery_cs_219_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_54 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_54_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_54
