{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH SmartBendingRep_67 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Barrier_Bend_Monitoring_1_', 'SmartBendingRep_67') }}

),

Formula_71_0 AS (

  SELECT 
    CAST('PUT2FWD_NOKO' AS string) AS `Smart Bending`,
    *
  
  FROM SmartBendingRep_67 AS in0

),

AlteryxSelect_68 AS (

  SELECT 
    `Smart Bending` AS `Smart Bending`,
    CAST(`Inmt Id` AS string) AS `Inmt Id`,
    Position AS Position,
    `$FV Legal` AS `$FV Legal`,
    `$FV Bent` AS `$FV Bent`,
    `$FV Unbent` AS `$FV Unbent`,
    `Exit?` AS `Exit?`
  
  FROM Formula_71_0 AS in0

),

SmartBendingRep_64 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Barrier_Bend_Monitoring_1_', 'SmartBendingRep_64') }}

),

Formula_70_0 AS (

  SELECT 
    CAST('ASSET_FILTERING' AS string) AS `Smart Bending`,
    *
  
  FROM SmartBendingRep_64 AS in0

),

AlteryxSelect_65 AS (

  SELECT 
    `Smart Bending` AS `Smart Bending`,
    CAST(`Inmt Id` AS string) AS `Inmt Id`,
    Position AS Position,
    `$FV Legal` AS `$FV Legal`,
    `$FV Bent` AS `$FV Bent`,
    `$FV Unbent` AS `$FV Unbent`,
    `$FV Bent(Rainbow)` AS `$FV Bent(Rainbow)`,
    `Exit?` AS `Exit?`
  
  FROM Formula_70_0 AS in0

),

Union_69 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_65', 'AlteryxSelect_68'], 
      [
        '[{"name": "Smart Bending", "dataType": "String"}, {"name": "Inmt Id", "dataType": "String"}, {"name": "Position", "dataType": "Double"}, {"name": "$FV Legal", "dataType": "Double"}, {"name": "$FV Bent", "dataType": "Double"}, {"name": "$FV Unbent", "dataType": "Double"}, {"name": "$FV Bent(Rainbow)", "dataType": "Double"}, {"name": "Exit?", "dataType": "Boolean"}]', 
        '[{"name": "Smart Bending", "dataType": "String"}, {"name": "Inmt Id", "dataType": "String"}, {"name": "Position", "dataType": "Double"}, {"name": "$FV Legal", "dataType": "Double"}, {"name": "$FV Bent", "dataType": "Double"}, {"name": "$FV Unbent", "dataType": "Double"}, {"name": "Exit?", "dataType": "Boolean"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_72_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((`$FV Bent(Rainbow)` IS NULL) AS BOOLEAN)
          THEN (`$FV Bent` - `$FV Unbent`)
        ELSE (`$FV Bent` - `$FV Bent(Rainbow)`)
      END
    ) AS DOUBLE) AS ` FV Impact  Smart Bending`,
    *
  
  FROM Union_69 AS in0

),

Formula_72_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((`$FV Bent(Rainbow)` IS NULL) AS BOOLEAN)
          THEN (ABS((` FV Impact  Smart Bending` / Position)) * 100)
        ELSE (((`$FV Bent` - `$FV Bent(Rainbow)`) / Position) * 100)
      END
    ) AS DOUBLE) AS ` %FV Impact  Smart Bending`,
    CAST((
      CASE
        WHEN CAST((`$FV Bent(Rainbow)` IS NULL) AS BOOLEAN)
          THEN (`$FV Unbent` - `$FV Legal`)
        ELSE (`$FV Unbent` - `$FV Bent(Rainbow)`)
      END
    ) AS DOUBLE) AS `Legal/Risky`,
    *
  
  FROM Formula_72_0 AS in0

),

Formula_72_2 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`Smart Bending` = 'ASSET_FILTERING')
          THEN (
            CASE
              WHEN ((` %FV Impact  Smart Bending` > 1) AND (ABS(` FV Impact  Smart Bending`) > 50000))
                THEN 'Smart bending Breach Asstefiltering'
              ELSE 'no Breach'
            END
          )
        WHEN ((` %FV Impact  Smart Bending` > 2) AND (ABS(` FV Impact  Smart Bending`) > 100000))
          THEN 'Smart bending Breach'
        ELSE 'no Breach'
      END
    ) AS string) AS `Smart Bend TH Breach`,
    CAST((
      CASE
        WHEN (
          (
            NOT(
              ` FV Impact  Smart Bending` = 0)
          ) OR (` FV Impact  Smart Bending` IS NULL)
        )
          THEN 'Y'
        ELSE 'N'
      END
    ) AS string) AS `Smart Bending (Y/N)`,
    *
  
  FROM Formula_72_1 AS in0

),

Formula_72_3 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`Smart Bend TH Breach` = 'no Breach')
          THEN 0
        ELSE 1
      END
    ) AS string) AS `Count of Inm>(2% & $100k)`,
    *
  
  FROM Formula_72_2 AS in0

)

SELECT *

FROM Formula_72_3
