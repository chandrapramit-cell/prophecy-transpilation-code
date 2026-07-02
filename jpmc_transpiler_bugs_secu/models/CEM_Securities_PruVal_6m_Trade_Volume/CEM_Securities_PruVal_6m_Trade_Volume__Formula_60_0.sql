{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_26 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__Union_26')}}

),

Summarize_68 AS (

  SELECT 
    DISTINCT `Counterparty Name` AS `Counterparty Name`,
    `Y/N?` AS `Y/N?`
  
  FROM Union_26 AS in0

),

Formula_60_0 AS (

  SELECT 
    CAST('Counterparty Mapping' AS string) AS Name,
    *
  
  FROM Summarize_68 AS in0

)

SELECT *

FROM Formula_60_0
