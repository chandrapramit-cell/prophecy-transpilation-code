{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_43_0 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__Formula_43_0')}}

),

Filter_34 AS (

  SELECT * 
  
  FROM Formula_43_0 AS in0
  
  WHERE (((ISIN IS NULL) OR (ISIN IS NULL)) OR ((LENGTH(ISIN)) = 0))

)

SELECT *

FROM Filter_34
