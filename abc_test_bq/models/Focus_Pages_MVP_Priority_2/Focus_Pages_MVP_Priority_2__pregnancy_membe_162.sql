{{
  config({    
    "materialized": "table",
    "alias": "pregnancy_membe_162_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH Unique_127 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Unique_127')}}

),

Sort_112 AS (

  SELECT * 
  
  FROM Unique_127 AS in0
  
  ORDER BY MBR_ID ASC

)

SELECT *

FROM Sort_112
