{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH `GUARATEE_KO$` AS (

  {#Overwrites the BarrierBendingM_5_ref table from transpiled_sources to publish updated barrier-bending reference data for downstream analysis.#}
  SELECT * 
  
  FROM {{ source('transpiled_sources', 'BarrierBendingM_5_ref') }}

),

Formula_81_0 AS (

  SELECT 
    CAST('GUARATEE_KO' AS string) AS `Smart Bending`,
    *
  
  FROM `GUARATEE_KO$` AS in0

),

AlteryxSelect_80 AS (

  SELECT 
    `Smart Bending` AS `Smart Bending`,
    `Inmt Id` AS `Inmt Id`,
    `$FV Bent` AS `$FV Bent`,
    `$FV Unbent` AS `$FV Unbent`
  
  FROM Formula_81_0 AS in0

)

SELECT *

FROM AlteryxSelect_80
