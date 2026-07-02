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

Filter_50 AS (

  SELECT * 
  
  FROM Union_26 AS in0
  
  WHERE (Region = 'ASIA')

)

SELECT *

FROM Filter_50
