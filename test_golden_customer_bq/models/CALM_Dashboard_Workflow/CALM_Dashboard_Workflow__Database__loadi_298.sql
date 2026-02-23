{{
  config({    
    "materialized": "table",
    "alias": "Database__loadi_298_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH Database__loadi_234 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__loadi_234_ref') }}

)

SELECT *

FROM Database__loadi_234
