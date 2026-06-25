{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_124 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1__Summarize_124')}}

),

Filter_206 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_1__Filter_206')}}

),

Join_107_left AS (

  SELECT in0.*
  
  FROM Filter_206 AS in0
  ANTI JOIN Summarize_124 AS in1
     ON ((CAST(in0.Date_fx AS DATE) = in1.variableDate) AND (in0.Denominated = in1.Currency))

),

Summarize_190 AS (

  SELECT 
    DISTINCT `Trade Date` AS `Trade Date`,
    Denominated AS Denominated
  
  FROM Join_107_left AS in0

),

Formula_200_0 AS (

  SELECT 
    CAST('Trade date with missing FX Rate' AS string) AS Name,
    *
  
  FROM Summarize_190 AS in0

)

SELECT *

FROM Formula_200_0
