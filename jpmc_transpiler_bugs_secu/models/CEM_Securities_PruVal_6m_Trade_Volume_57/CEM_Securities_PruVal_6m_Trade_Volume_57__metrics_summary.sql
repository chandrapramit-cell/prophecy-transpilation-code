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

metrics_summary AS (

  SELECT 
    SUM(CAST(REPLACE(REPLACE(`Notional (USD)`, ',', ''), '$', '') AS DOUBLE)) AS total_notional,
    SUM(CAST(REPLACE(REPLACE(`Consideration (USD)`, ',', ''), '$', '') AS DOUBLE)) AS total_consideration,
    COUNT(*) AS total_trades
  
  FROM GEM_Credit_Trad_1_57 AS source_data

)

SELECT *

FROM metrics_summary
