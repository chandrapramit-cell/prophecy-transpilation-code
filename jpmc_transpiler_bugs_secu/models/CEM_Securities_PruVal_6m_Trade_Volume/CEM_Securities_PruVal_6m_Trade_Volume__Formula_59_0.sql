{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Filter_50 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__Filter_50')}}

),

Summarize_13 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__Summarize_13')}}

),

Join_25_left AS (

  SELECT in0.*
  
  FROM Filter_50 AS in0
  ANTI JOIN Summarize_13 AS in1
     ON ((CAST(in0.Date_fx AS DATE) = in1.variableDate) AND (in0.Denominated = in1.Currency))

),

Summarize_69 AS (

  SELECT 
    DISTINCT `Trade Date` AS `Trade Date`,
    Denominated AS Denominated
  
  FROM Join_25_left AS in0

),

Formula_59_0 AS (

  SELECT 
    CAST('Trade date with missing FX Rate' AS string) AS Name,
    *
  
  FROM Summarize_69 AS in0

)

SELECT *

FROM Formula_59_0
