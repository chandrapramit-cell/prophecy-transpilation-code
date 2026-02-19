{{
  config({    
    "materialized": "table",
    "alias": "vague_msk_csv_215_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_53 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_53_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_53
