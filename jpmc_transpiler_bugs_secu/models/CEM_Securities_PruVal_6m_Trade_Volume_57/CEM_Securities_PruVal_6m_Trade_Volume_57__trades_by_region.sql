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

trades_by_region AS (

  SELECT 
    Region,
    COUNT(*) AS trade_count,
    SUM(CAST(REPLACE(REPLACE(`Notional (USD)`, ',', ''), '$', '') AS DOUBLE)) AS total_notional_usd,
    SUM(CAST(REPLACE(REPLACE(`Consideration (USD)`, ',', ''), '$', '') AS DOUBLE)) AS total_consideration_usd
  
  FROM GEM_Credit_Trad_1_57 AS source_data
  
  GROUP BY Region

)

SELECT *

FROM trades_by_region
