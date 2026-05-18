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
    `dollarFV Legal` AS `dollarFV Legal`,
    `dollarFV Bent` AS `dollarFV Bent`,
    `dollarFV Unbent` AS `dollarFV Unbent`,
    ExitquesMark AS ExitquesMark
  
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
    `dollarFV Legal` AS `dollarFV Legal`,
    `dollarFV Bent` AS `dollarFV Bent`,
    `dollarFV Unbent` AS `dollarFV Unbent`,
    `dollarFV BentparanthesesOpenRainbowparanthesesClose` AS `dollarFV BentparanthesesOpenRainbowparanthesesClose`,
    ExitquesMark AS ExitquesMark
  
  FROM Formula_70_0 AS in0

),

Union_69 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_65', 'AlteryxSelect_68'], 
      [
        '[{"name": "Smart Bending", "dataType": "String"}, {"name": "Inmt Id", "dataType": "String"}, {"name": "Position", "dataType": "Double"}, {"name": "dollarFV Legal", "dataType": "Double"}, {"name": "dollarFV Bent", "dataType": "Double"}, {"name": "dollarFV Unbent", "dataType": "Double"}, {"name": "dollarFV BentparanthesesOpenRainbowparanthesesClose", "dataType": "Double"}, {"name": "ExitquesMark", "dataType": "Boolean"}]', 
        '[{"name": "Smart Bending", "dataType": "String"}, {"name": "Inmt Id", "dataType": "String"}, {"name": "Position", "dataType": "Double"}, {"name": "dollarFV Legal", "dataType": "Double"}, {"name": "dollarFV Bent", "dataType": "Double"}, {"name": "dollarFV Unbent", "dataType": "Double"}, {"name": "ExitquesMark", "dataType": "Boolean"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_72_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((`dollarFV BentparanthesesOpenRainbowparanthesesClose` IS NULL) AS BOOLEAN)
          THEN (`dollarFV Bent` - `dollarFV Unbent`)
        ELSE (`dollarFV Bent` - `dollarFV BentparanthesesOpenRainbowparanthesesClose`)
      END
    ) AS DOUBLE) AS ` FV Impact  Smart Bending`,
    *
  
  FROM Union_69 AS in0

),

Formula_72_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((`dollarFV BentparanthesesOpenRainbowparanthesesClose` IS NULL) AS BOOLEAN)
          THEN (ABS((` FV Impact  Smart Bending` / Position)) * 100)
        ELSE (((`dollarFV Bent` - `dollarFV BentparanthesesOpenRainbowparanthesesClose`) / Position) * 100)
      END
    ) AS DOUBLE) AS ` percentFV Impact  Smart Bending`,
    CAST((
      CASE
        WHEN CAST((`dollarFV BentparanthesesOpenRainbowparanthesesClose` IS NULL) AS BOOLEAN)
          THEN (`dollarFV Unbent` - `dollarFV Legal`)
        ELSE (`dollarFV Unbent` - `dollarFV BentparanthesesOpenRainbowparanthesesClose`)
      END
    ) AS DOUBLE) AS LegalslashRisky,
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
              WHEN ((` percentFV Impact  Smart Bending` > 1) AND (ABS(` FV Impact  Smart Bending`) > 50000))
                THEN 'Smart bending Breach Asstefiltering'
              ELSE 'no Breach'
            END
          )
        WHEN ((` percentFV Impact  Smart Bending` > 2) AND (ABS(` FV Impact  Smart Bending`) > 100000))
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
    ) AS string) AS `Smart Bending paranthesesOpenYslashNparanthesesClose`,
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
    ) AS string) AS `Count of Inm>paranthesesOpen2percent ampersand dollar100kparanthesesClose`,
    *
  
  FROM Formula_72_2 AS in0

)

SELECT *

FROM Formula_72_3
