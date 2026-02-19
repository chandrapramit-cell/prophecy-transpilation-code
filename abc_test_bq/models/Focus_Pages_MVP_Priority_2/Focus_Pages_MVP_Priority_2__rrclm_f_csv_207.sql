{{
  config({    
    "materialized": "table",
    "alias": "rrclm_f_csv_207_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_116 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_116_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_116
