{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_226 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1___AlteryxSelect_226')}}

),

Unique_227_window AS (

  SELECT 
    *,
    row_number() OVER (PARTITION BY variableDate, Currency ORDER BY variableDate ASC NULLS FIRST, Currency ASC NULLS FIRST) AS row_number
  
  FROM AlteryxSelect_226 AS in0

),

Unique_227_filter AS (

  SELECT * 
  
  FROM Unique_227_window AS in0
  
  WHERE (row_number > 1)

),

Unique_227_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM Unique_227_filter AS in0

),

Summarize_228 AS (

  SELECT 
    DISTINCT variableDate AS variableDate,
    Currency AS Currency
  
  FROM Unique_227_drop_0 AS in0

),

Formula_229_0 AS (

  SELECT 
    CAST('Duplicate FX Rate' AS string) AS Name,
    *
  
  FROM Summarize_228 AS in0

)

SELECT *

FROM Formula_229_0
