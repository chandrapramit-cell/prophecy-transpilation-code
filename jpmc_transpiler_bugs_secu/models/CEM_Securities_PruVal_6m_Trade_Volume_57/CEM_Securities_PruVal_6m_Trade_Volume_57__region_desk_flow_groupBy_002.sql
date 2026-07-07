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

region_desk_flow_from_000 AS (

  SELECT 
    Region,
    Desk
  
  FROM GEM_Credit_Trad_1_57 AS source_data

),

region_desk_flow_filter_001 AS (

  SELECT * 
  
  FROM region_desk_flow_from_000
  
  WHERE Region IS NOT NULL AND Desk IS NOT NULL

),

region_desk_flow_groupBy_002 AS (

  SELECT 
    Region AS source_node,
    Desk AS target_node,
    COUNT(*) AS flow_value
  
  FROM region_desk_flow_filter_001
  
  GROUP BY 
    Region, Desk

)

SELECT *

FROM region_desk_flow_groupBy_002
