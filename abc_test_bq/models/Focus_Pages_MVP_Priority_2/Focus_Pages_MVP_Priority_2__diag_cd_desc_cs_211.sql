{{
  config({    
    "materialized": "table",
    "alias": "diag_cd_desc_cs_211_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_119 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_119_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_119
