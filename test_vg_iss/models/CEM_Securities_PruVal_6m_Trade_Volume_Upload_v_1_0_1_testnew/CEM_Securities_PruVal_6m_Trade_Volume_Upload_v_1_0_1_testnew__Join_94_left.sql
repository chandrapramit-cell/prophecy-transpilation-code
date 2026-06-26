{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_96 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__AlteryxSelect_96')}}

),

Filter_93 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Filter_93')}}

),

Join_94_left AS (

  SELECT in0.*
  
  FROM Filter_93 AS in0
  ANTI JOIN AlteryxSelect_96 AS in1
     ON (in0.`Instrument Description` = in1.`Instrument Description`)

)

SELECT *

FROM Join_94_left
