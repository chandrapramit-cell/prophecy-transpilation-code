{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_88 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1', 'Configuration_t_88') }}

),

AlteryxSelect_96 AS (

  SELECT 
    `Bond description` AS `Instrument Description`,
    * EXCEPT (`Bond description`)
  
  FROM Configuration_t_88 AS in0

)

SELECT *

FROM AlteryxSelect_96
