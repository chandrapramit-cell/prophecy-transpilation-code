{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH SmartBendingRep_79 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Barrier_Bend_Monitoring_1_', 'SmartBendingRep_79') }}

),

Formula_81_0 AS (

  SELECT 
    CAST('GUARATEE_KO' AS string) AS `Smart Bending`,
    *
  
  FROM SmartBendingRep_79 AS in0

),

AlteryxSelect_80 AS (

  SELECT 
    `Smart Bending` AS `Smart Bending`,
    `Inmt Id` AS `Inmt Id`,
    `dollarFV Bent` AS `dollarFV Bent`,
    `dollarFV Unbent` AS `dollarFV Unbent`
  
  FROM Formula_81_0 AS in0

)

SELECT *

FROM AlteryxSelect_80
