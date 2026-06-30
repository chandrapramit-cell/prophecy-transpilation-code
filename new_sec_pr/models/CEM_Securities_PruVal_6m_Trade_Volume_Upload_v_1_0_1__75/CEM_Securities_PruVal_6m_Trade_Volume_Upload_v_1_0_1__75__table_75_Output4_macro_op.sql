{{
  config({    
    "materialized": "incremental",
    "alias": var('table_75_Output4_macro_op'),
    "database": var('db_name'),
    "incremental_strategy": "append",
    "schema": var('schema_name')
  })
}}

WITH VCGBondVolumesR_1_75 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'VCGBondVolumesR_1_75_ref') }}

)

SELECT *

FROM VCGBondVolumesR_1_75
