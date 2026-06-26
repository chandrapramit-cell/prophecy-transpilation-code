{{
  config({    
    "materialized": "incremental",
    "alias": "table_79_Output4_macro_op",
    "database": "sony",
    "incremental_strategy": "append",
    "schema": "orch_test"
  })
}}

WITH GEM_Credit_Trad_1_79 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_321414_79', 
      'GEM_Credit_Trad_1_79'
    )
  }}

)

SELECT *

FROM GEM_Credit_Trad_1_79
