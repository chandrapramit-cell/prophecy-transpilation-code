{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_28 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_', 'Configuration_t_28') }}

),

Formula_50_0 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1___Formula_50_0')}}

),

Join_35_left AS (

  SELECT in0.*
  
  FROM Formula_50_0 AS in0
  ANTI JOIN Configuration_t_28 AS in1
     ON (in0.INSTRUMENTCODE = in1.`INSTRUMENT CODE`)

),

Summarize_212 AS (

  SELECT DISTINCT INSTRUMENTCODE AS INSTRUMENTCODE
  
  FROM Join_35_left AS in0

),

Formula_213_0 AS (

  SELECT 
    CAST('Missing Product type' AS string) AS Name,
    *
  
  FROM Summarize_212 AS in0

)

SELECT *

FROM Formula_213_0
