{{
  config({    
    "materialized": "table",
    "alias": "high_risk_preg__237_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH aka_alxaa2_Quer_134 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'aka_alxaa2_Quer_134_ref') }}

)

SELECT *

FROM aka_alxaa2_Quer_134
