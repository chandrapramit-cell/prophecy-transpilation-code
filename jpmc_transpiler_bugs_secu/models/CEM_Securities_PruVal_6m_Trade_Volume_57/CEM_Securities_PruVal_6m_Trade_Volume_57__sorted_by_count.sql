{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH GEM_Credit_Trad_1_57 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume_57', 'GEM_Credit_Trad_1_57') }}

),

trades_by_status AS (

  SELECT 
    Status AS funnel_stage,
    COUNT(*) AS trade_count
  
  FROM GEM_Credit_Trad_1_57 AS source_data
  
  GROUP BY Status

),

sorted_by_count AS (

  SELECT * 
  
  FROM trades_by_status
  
  ORDER BY trade_count DESC

)

SELECT *

FROM sorted_by_count
