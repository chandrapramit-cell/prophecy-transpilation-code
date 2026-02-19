{{
  config({    
    "materialized": "table",
    "alias": "shoulder_surger_223_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_56 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_56_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_56
