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

scatter_data_from_000 AS (

  SELECT 
    `Trade ID` AS trade_id,
    CAST(REPLACE(REPLACE(`Notional (USD)`, ',', ''), '$', '') AS DOUBLE) AS notional_usd,
    CAST(REPLACE(REPLACE(`Consideration (USD)`, ',', ''), '$', '') AS DOUBLE) AS consideration_usd,
    CAST(REPLACE(REPLACE(`Clean Price`, ',', ''), '$', '') AS DOUBLE) AS clean_price
  
  FROM GEM_Credit_Trad_1_57 AS source_data

),

scatter_data_filter_001 AS (

  SELECT * 
  
  FROM scatter_data_from_000
  
  WHERE notional_usd IS NOT NULL AND consideration_usd IS NOT NULL

),

scatter_data_projection_002 AS (

  SELECT 
    trade_id,
    notional_usd,
    consideration_usd,
    clean_price
  
  FROM scatter_data_filter_001

)

SELECT *

FROM scatter_data_projection_002
