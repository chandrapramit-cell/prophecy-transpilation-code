{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_37 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume', 'Configuration_t_37') }}

),

AlteryxSelect_31 AS (

  SELECT 
    `Bond Description` AS `Instrument Description`,
    * EXCEPT (`Bond description`)
  
  FROM Configuration_t_37 AS in0

)

SELECT *

FROM AlteryxSelect_31
