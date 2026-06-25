{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_16 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1__Summarize_16')}}

),

Configuration_t_14 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1', 'Configuration_t_14') }}

),

Join_15_left AS (

  SELECT in0.*
  
  FROM Summarize_16 AS in0
  ANTI JOIN Configuration_t_14 AS in1
     ON (in0.ISIN = in1.ISIN)

),

Summarize_217 AS (

  SELECT DISTINCT ISIN AS ISIN
  
  FROM Join_15_left AS in0

),

Formula_218_0 AS (

  SELECT 
    CAST('Missing Product type' AS string) AS Name,
    *
  
  FROM Summarize_217 AS in0

)

SELECT *

FROM Formula_218_0
