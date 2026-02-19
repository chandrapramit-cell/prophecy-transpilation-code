{{
  config({    
    "materialized": "table",
    "alias": "diabetes_csv_47_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH Formula_80_1 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Formula_80_1')}}

)

SELECT *

FROM Formula_80_1
