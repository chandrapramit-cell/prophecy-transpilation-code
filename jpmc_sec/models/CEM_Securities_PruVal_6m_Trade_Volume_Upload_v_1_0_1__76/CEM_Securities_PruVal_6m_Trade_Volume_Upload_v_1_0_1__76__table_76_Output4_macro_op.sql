{{
  config({    
    "materialized": "incremental",
    "alias": "table_76_Output4_macro_op",
    "database": var('db_name'),
    "incremental_strategy": "append",
    "schema": var('schema_name')
  })
}}

WITH GEM_Credit_Trad_1_76 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'GEM_Credit_Trad_1_76_ref') }}

)

SELECT *

FROM GEM_Credit_Trad_1_76
