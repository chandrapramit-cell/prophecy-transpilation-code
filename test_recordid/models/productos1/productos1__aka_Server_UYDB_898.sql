{{
  config({    
    "materialized": "table",
    "alias": "aka_Server_UYDB_898",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_895 AS (

  SELECT *
  
  FROM {{ ref('productos1__Union_895')}}

),

Sample_897 AS (

  {{ prophecy_basics.Sample('', [], 1002, 'firstN', 1) }}

)

SELECT *

FROM Sample_897
