{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_33_left AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__Join_33_left')}}

),

Summarize_67 AS (

  SELECT DISTINCT `Instrument Description` AS `Instrument Description`
  
  FROM Join_33_left AS in0

),

Formula_61_0 AS (

  SELECT 
    CAST('Instru Desc. without ISIN' AS string) AS Name,
    *
  
  FROM Summarize_67 AS in0

)

SELECT *

FROM Formula_61_0
