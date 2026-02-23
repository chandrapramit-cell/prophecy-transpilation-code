{{
  config({    
    "materialized": "table",
    "alias": "CCI_Break_Out_c_80_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH Unique_78 AS (

  SELECT *
  
  FROM {{ ref('CCI__Unique_78')}}

)

SELECT *

FROM Unique_78
