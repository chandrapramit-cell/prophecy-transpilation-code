{{
  config({    
    "materialized": "incremental",
    "alias": "table_76_Output4_macro_op",
    "database": "sony",
    "incremental_strategy": "append",
    "schema": "orch_test"
  })
}}

WITH GEM_Credit_Trad_1_76 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew_76', 
      'GEM_Credit_Trad_1_76'
    )
  }}

)

SELECT *

FROM GEM_Credit_Trad_1_76
