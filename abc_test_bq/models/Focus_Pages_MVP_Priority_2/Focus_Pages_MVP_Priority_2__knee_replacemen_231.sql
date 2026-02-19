{{
  config({    
    "materialized": "table",
    "alias": "knee_replacemen_231_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_57 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_57_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_57
