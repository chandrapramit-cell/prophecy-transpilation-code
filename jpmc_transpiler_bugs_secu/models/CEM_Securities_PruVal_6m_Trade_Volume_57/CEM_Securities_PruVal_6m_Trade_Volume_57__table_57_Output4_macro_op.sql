{{
  config({    
    "materialized": "incremental",
    "alias": "table_57_Output4_macro_op",
    "database": "sony",
    "incremental_strategy": "append",
    "schema": "orch_test"
  })
}}

WITH GEM_Credit_Trad_1_57 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume_57', 'GEM_Credit_Trad_1_57') }}

)

SELECT *

FROM GEM_Credit_Trad_1_57
