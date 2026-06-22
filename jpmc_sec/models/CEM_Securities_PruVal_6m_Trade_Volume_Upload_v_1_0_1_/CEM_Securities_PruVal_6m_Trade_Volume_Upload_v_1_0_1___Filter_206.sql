{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_103 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1___Union_103')}}

),

Filter_206 AS (

  SELECT * 
  
  FROM Union_103 AS in0
  
  WHERE (Region = 'ASIA')

)

SELECT *

FROM Filter_206
