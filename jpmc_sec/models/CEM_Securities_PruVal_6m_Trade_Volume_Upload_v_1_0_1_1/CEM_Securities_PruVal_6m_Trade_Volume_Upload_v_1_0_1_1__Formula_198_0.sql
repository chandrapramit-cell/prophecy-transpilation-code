{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_94_left AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1__Join_94_left')}}

),

Summarize_188 AS (

  SELECT DISTINCT `Instrument Description` AS `Instrument Description`
  
  FROM Join_94_left AS in0

),

Formula_198_0 AS (

  SELECT 
    CAST('Instru Desc. without ISIN' AS string) AS Name,
    *
  
  FROM Summarize_188 AS in0

)

SELECT *

FROM Formula_198_0
