{{
  config({    
    "materialized": "table",
    "alias": "Database__LOADI_98_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH Unique_97 AS (

  SELECT *
  
  FROM {{ ref('boeing__Unique_97')}}

)

SELECT *

FROM Unique_97
