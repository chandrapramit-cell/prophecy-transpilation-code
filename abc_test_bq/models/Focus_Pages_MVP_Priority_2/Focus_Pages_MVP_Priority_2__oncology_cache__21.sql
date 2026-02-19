{{
  config({    
    "materialized": "table",
    "alias": "oncology_cache__21_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_1 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_1_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_1
