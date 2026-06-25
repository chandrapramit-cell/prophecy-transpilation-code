{{
  config({    
    "materialized": "incremental",
    "alias": "table_79_Output4_macro_op",
    "database": var('db_name'),
    "incremental_strategy": "append",
    "schema": var('schema_name')
  })
}}

WITH GEM_Credit_Trad_1_79 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'GEM_Credit_Trad_1_79_ref') }}

)

SELECT *

FROM GEM_Credit_Trad_1_79
