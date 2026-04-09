{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Cleanse_316 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Cleanse_316')}}

),

Cleanse_317 AS (

  SELECT *
  
  FROM {{ ref('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4__Cleanse_317')}}

),

Join_195_left AS (

  SELECT in0.*
  
  FROM Cleanse_317 AS in0
  LEFT JOIN Cleanse_316 AS in1
     ON (in0.SOURCE_WH_DESC = in1.SOURCEWHDESCRIPTION)

)

SELECT *

FROM Join_195_left
