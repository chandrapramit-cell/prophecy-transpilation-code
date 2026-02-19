{{
  config({    
    "materialized": "table",
    "alias": "mother_birthing_199_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_98 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_98_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_98
