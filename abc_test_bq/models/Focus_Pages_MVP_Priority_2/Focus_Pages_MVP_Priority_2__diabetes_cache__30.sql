{{
  config({    
    "materialized": "table",
    "alias": "diabetes_cache__30_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_29 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_29_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_29
