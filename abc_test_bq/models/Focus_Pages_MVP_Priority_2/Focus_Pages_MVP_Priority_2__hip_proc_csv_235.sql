{{
  config({    
    "materialized": "table",
    "alias": "hip_proc_csv_235_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_58 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_58_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_58
