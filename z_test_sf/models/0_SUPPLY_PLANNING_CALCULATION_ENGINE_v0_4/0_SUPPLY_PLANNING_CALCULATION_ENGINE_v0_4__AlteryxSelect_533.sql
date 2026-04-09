{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH SalesForecastMa_532 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('0_SUPPLY_PLANNING_CALCULATION_ENGINE_v0_4', 'SalesForecastMa_532') }}

),

AlteryxSelect_533 AS (

  SELECT * EXCLUDE ("F2", "F3", "F4", "F5", "F6", "F7")
  
  FROM SalesForecastMa_532 AS in0

)

SELECT *

FROM AlteryxSelect_533
