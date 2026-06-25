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
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1_76', 
      'GEM_Credit_Trad_1_76'
    )
  }}

)

SELECT *

FROM GEM_Credit_Trad_1_76
