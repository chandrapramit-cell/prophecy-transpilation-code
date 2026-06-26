{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_82_0 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_82_0')}}

),

Filter_93 AS (

  SELECT * 
  
  FROM Formula_82_0 AS in0
  
  WHERE (((ISIN IS NULL) OR (ISIN IS NULL)) OR ((LENGTH(ISIN)) = 0))

)

SELECT *

FROM Filter_93
