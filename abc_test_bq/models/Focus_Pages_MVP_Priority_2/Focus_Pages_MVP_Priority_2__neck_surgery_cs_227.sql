{{
  config({    
    "materialized": "table",
    "alias": "neck_surgery_cs_227_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_55 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_55_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_55
