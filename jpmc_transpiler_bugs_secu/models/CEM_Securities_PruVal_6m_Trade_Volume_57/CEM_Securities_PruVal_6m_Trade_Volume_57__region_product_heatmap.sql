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

region_product_heatmap AS (

  SELECT 
    Region,
    `Product Type` AS product_type,
    COUNT(*) AS trade_count
  
  FROM GEM_Credit_Trad_1_57 AS source_data
  
  GROUP BY 
    Region, `Product Type`

)

SELECT *

FROM region_product_heatmap
