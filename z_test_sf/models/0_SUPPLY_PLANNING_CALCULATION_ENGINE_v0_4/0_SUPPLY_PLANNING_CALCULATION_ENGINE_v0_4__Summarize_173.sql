{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Union_160 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Union_160')}}

),

Summarize_173 AS (

  SELECT DISTINCT SOURCE_WH_DESC AS SOURCE_WH_DESC
  
  FROM Union_160 AS in0

)

SELECT *

FROM Summarize_173
