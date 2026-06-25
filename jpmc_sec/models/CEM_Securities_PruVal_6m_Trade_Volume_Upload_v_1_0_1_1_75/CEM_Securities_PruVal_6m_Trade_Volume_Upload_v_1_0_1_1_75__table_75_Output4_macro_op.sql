{{
  config({    
    "materialized": "incremental",
    "alias": "table_75_Output4_macro_op",
    "database": var('db_name'),
    "incremental_strategy": "append",
    "schema": var('schema_name')
  })
}}

WITH VCGBondVolumesR_1_75 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1_75', 
      'VCGBondVolumesR_1_75'
    )
  }}

)

SELECT *

FROM VCGBondVolumesR_1_75
