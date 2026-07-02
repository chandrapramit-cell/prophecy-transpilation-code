{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_36 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume', 'Configuration_t_36') }}

),

Summarize_13 AS (

  SELECT 
    DISTINCT variableDate AS variableDate,
    Currency AS Currency,
    FX AS FX
  
  FROM Configuration_t_36 AS in0

)

SELECT *

FROM Summarize_13
