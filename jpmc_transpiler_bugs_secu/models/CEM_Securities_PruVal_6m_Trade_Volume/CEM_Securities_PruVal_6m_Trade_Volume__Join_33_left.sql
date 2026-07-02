{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_31 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__AlteryxSelect_31')}}

),

Filter_34 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume__Filter_34')}}

),

Join_33_left AS (

  SELECT in0.*
  
  FROM Filter_34 AS in0
  ANTI JOIN AlteryxSelect_31 AS in1
     ON (in0.`Instrument Description` = in1.`Instrument Description`)

)

SELECT *

FROM Join_33_left
