{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_215_0 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Formula_215_0')}}

),

Union_198 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Union_198')}}

),

Join_202_left AS (

  SELECT in0.*
  
  FROM Union_198 AS in0
  LEFT JOIN Formula_215_0 AS in1
     ON (in0.SOURCE_SKU = in1.SOURCE_SKU)

)

SELECT *

FROM Join_202_left
