{{
  config({    
    "materialized": "table",
    "alias": "batch_macro_61_in0",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH AppendFields_67 AS (

  SELECT *
  
  FROM {{ ref('p1__AppendFields_67')}}

)

SELECT *

FROM AppendFields_67
