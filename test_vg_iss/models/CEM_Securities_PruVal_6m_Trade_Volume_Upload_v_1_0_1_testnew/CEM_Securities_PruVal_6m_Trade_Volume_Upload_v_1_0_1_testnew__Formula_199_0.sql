{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_103 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Union_103')}}

),

Summarize_189 AS (

  SELECT 
    DISTINCT `Counterparty Name` AS `Counterparty Name`,
    `Y/N?` AS `Y/N?`
  
  FROM Union_103 AS in0

),

Formula_199_0 AS (

  SELECT 
    CAST('Counterparty Mapping' AS string) AS Name,
    *
  
  FROM Summarize_189 AS in0

)

SELECT *

FROM Formula_199_0
