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

trades_by_date AS (

  SELECT 
    `Trade Date` AS trade_date,
    COUNT(*) AS trade_count,
    SUM(CAST(REPLACE(REPLACE(`Notional (USD)`, ',', ''), '$', '') AS DOUBLE)) AS total_notional_usd
  
  FROM GEM_Credit_Trad_1_57 AS source_data
  
  GROUP BY `Trade Date`

),

sorted_by_date AS (

  SELECT * 
  
  FROM trades_by_date
  
  ORDER BY trade_date ASC

)

SELECT *

FROM sorted_by_date
