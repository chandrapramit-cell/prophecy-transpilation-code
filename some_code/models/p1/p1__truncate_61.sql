{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH AppendFields_67 AS (

  SELECT *
  
  FROM {{ ref('p1__AppendFields_67')}}

),

truncate_61 AS (

  -- Use the actual table name with the catalog and schema
  TRUNCATE TABLE batch_macro_61_AppendFields_67
  out0=in0

)

SELECT *

FROM truncate_61
