{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_338 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__AlteryxSelect_338')}}

),

Formula_237_0 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Formula_237_0')}}

),

Join_339_left AS (

  SELECT in0.*
  
  FROM Formula_237_0 AS in0
  LEFT JOIN AlteryxSelect_338 AS in1
     ON (in0.SKU_STANDARD = in1.SKU_STANDARD)

)

SELECT *

FROM Join_339_left
