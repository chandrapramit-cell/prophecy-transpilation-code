{{
  config({    
    "materialized": "table",
    "alias": "pregnancy_comp__236_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_167 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_167_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_167
