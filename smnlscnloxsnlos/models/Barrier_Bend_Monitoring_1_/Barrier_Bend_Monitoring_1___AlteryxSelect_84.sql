{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_85_0 AS (

  SELECT *
  
  FROM {{ ref('Barrier_Bend_Monitoring_1___Formula_85_0')}}

),

BarReport_APAC__1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Barrier_Bend_Monitoring_1_', 'BarReport_APAC__1') }}

),

Filter_54 AS (

  SELECT * 
  
  FROM Formula_85_0 AS in0
  
  WHERE (`Final- Breach or Pending QR or <TH` = 'Breach')

),

AlteryxSelect_52 AS (

  SELECT `Imnt Id` AS `Imnt Id`
  
  FROM Filter_54 AS in0

),

Join_51_left_UnionLeftOuter AS (

  SELECT 
    in0.`Asset Spot` AS `Asset Spot`,
    in0.`Legal Barrier Next` AS `Legal Barrier Next`,
    in0.`Risky Barrier Next` AS `Risky Barrier Next`,
    in0.`Legal Barrier At Max Disc` AS `Legal Barrier At Max Disc`,
    in0.`Risky Barrier At Max Disc` AS `Risky Barrier At Max Disc`,
    (
      CASE
        WHEN (in0.`Imnt Id` = in1.`Imnt Id`)
          THEN in1.`Imnt Id`
        ELSE NULL
      END
    ) AS `Breach Inmt ID`,
    in0.* EXCEPT (`Asset Spot`, 
    `Legal Barrier Next`, 
    `Risky Barrier Next`, 
    `Legal Barrier At Max Disc`, 
    `Risky Barrier At Max Disc`),
    in1.* EXCEPT (`Imnt Id`)
  
  FROM BarReport_APAC__1 AS in0
  LEFT JOIN AlteryxSelect_52 AS in1
     ON (in0.`Imnt Id` = in1.`Imnt Id`)

),

Formula_57_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((`Breach Inmt ID` IS NULL) AS BOOLEAN)
          THEN 'N'
        ELSE 'Y'
      END
    ) AS string) AS `Breach To investigate`,
    *
  
  FROM Join_51_left_UnionLeftOuter AS in0

),

AlteryxSelect_58 AS (

  SELECT 
    CAST(`Legal Barrier Next` AS DOUBLE) AS `Legal Barrier Next`,
    CAST(`Risky Barrier Next` AS DOUBLE) AS `Risky Barrier Next`,
    CAST(USDRiskyFV AS DOUBLE) AS USDRiskyFV,
    CAST(USDLegalFV AS DOUBLE) AS USDLegalFV,
    CAST(USDLegalRisky AS DOUBLE) AS USDLegalRisky,
    * EXCEPT (`Breach Inmt ID`, `Legal Barrier Next`, `Risky Barrier Next`, `USDRiskyFV`, `USDLegalFV`, `USDLegalRisky`)
  
  FROM Formula_57_0 AS in0

),

Filter_59 AS (

  SELECT * 
  
  FROM AlteryxSelect_58 AS in0
  
  WHERE (`Breach To investigate` = 'Y')

),

Summarize_50 AS (

  SELECT 
    SUM(USDLegalRisky) AS Sum_USDLegalRisky,
    `Imnt Id` AS `Imnt Id`,
    `Asset Pyramid Name` AS `Asset Pyramid Name`,
    MaxBends AS MaxBends,
    Desk AS Desk,
    `Relative Risk Levels` AS `Relative Risk Levels`,
    Bends AS Bends,
    `Asset Spot` AS `Asset Spot`,
    barType AS barType,
    `Option Class` AS `Option Class`,
    `Legal Barrier Next` AS `Legal Barrier Next`,
    hasStaticCheckMaxBreach AS hasStaticCheckMaxBreach,
    `Relative Legal Levels` AS `Relative Legal Levels`,
    `Legal Barrier At Max Disc` AS `Legal Barrier At Max Disc`
  
  FROM Filter_59 AS in0
  
  GROUP BY 
    `Imnt Id`, 
    `Asset Pyramid Name`, 
    MaxBends, 
    Desk, 
    `Relative Risk Levels`, 
    Bends, 
    `Asset Spot`, 
    barType, 
    `Option Class`, 
    `Legal Barrier Next`, 
    hasStaticCheckMaxBreach, 
    `Relative Legal Levels`, 
    `Legal Barrier At Max Disc`

),

Formula_63_0 AS (

  SELECT 
    CAST(((`Legal Barrier Next` / CAST(`Asset Spot` AS DOUBLE)) * 100) AS DOUBLE) AS `Next Barrier vs Current Spot`,
    CAST((MODULO(`Legal Barrier At Max Disc`, `Asset Spot`)) AS DOUBLE) AS `Barrier at max disc vs Current Spot`,
    CAST(NULL AS string) AS `VCG Accepted`,
    CAST((
      CONCAT(
        barType, 
        ' Barrier breached on relative strike @', 
        (SUBSTRING(`Relative Risk Levels`, (((LENGTH(`Relative Risk Levels`)) - 6) + 1), 6)), 
        ' But ATM bending is ', 
        (
          REGEXP_REPLACE(
            (
              REGEXP_REPLACE(
                (
                  FORMAT_NUMBER(
                    CAST((
                      (
                        coalesce(
                          CAST((SUBSTRING(`Relative Legal Levels`, (((LENGTH(`Relative Legal Levels`)) - 6) + 1), 6)) AS DOUBLE), 
                          CAST((REGEXP_EXTRACT((SUBSTRING(`Relative Legal Levels`, (((LENGTH(`Relative Legal Levels`)) - 6) + 1), 6)), '^[0-9]+', 0)) AS INTEGER), 
                          0)
                      )
                      - (
                          coalesce(
                            CAST((SUBSTRING(`Relative Risk Levels`, (((LENGTH(`Relative Risk Levels`)) - 6) + 1), 6)) AS DOUBLE), 
                            CAST((REGEXP_EXTRACT((SUBSTRING(`Relative Risk Levels`, (((LENGTH(`Relative Risk Levels`)) - 6) + 1), 6)), '^[0-9]+', 0)) AS INTEGER), 
                            0)
                        )
                    ) AS DOUBLE), 
                    0)
                ), 
                ',', 
                '__THS__')
            ), 
            '__THS__', 
            '')
        ), 
        '%')
    ) AS string) AS Comment,
    *
  
  FROM Summarize_50 AS in0

),

Join_82_left_UnionLeftOuter AS (

  SELECT 
    in1.`Smart Bending` AS `Smart Bending`,
    in1.`Smart Bending Instrument` AS `Smart Bending Instrument`,
    in0.`Asset Pyramid Name` AS `Asset Pyramid Name`,
    in0.`Asset Spot` AS `Asset Spot`,
    in0.`Legal Barrier Next` AS `Legal Barrier Next`,
    in0.`Legal Barrier At Max Disc` AS `Legal Barrier At Max Disc`,
    in0.hasStaticCheckMaxBreach AS hasStaticCheckMaxBreach,
    in0.MaxBends AS MaxBends,
    in0.Bends AS Bends,
    in0.`Relative Risk Levels` AS `Relative Risk Levels`,
    in0.`Relative Legal Levels` AS `Relative Legal Levels`,
    in0.Sum_USDLegalRisky AS Sum_USDLegalRisky,
    in0.Desk AS Desk,
    in0.`Imnt Id` AS `Imnt Id`,
    in0.`Option Class` AS `Option Class`,
    in0.barType AS barType,
    in0.`Next Barrier vs Current Spot` AS `Next Barrier vs Current Spot`,
    in0.`Barrier at max disc vs Current Spot` AS `Barrier at max disc vs Current Spot`,
    in0.`VCG Accepted` AS `VCG Accepted`,
    in0.Comment AS Comment,
    in1.`Smart Bending Breach` AS `Smart Bending Breach`,
    in0.* EXCEPT (`Asset Pyramid Name`, 
    `Asset Spot`, 
    `Legal Barrier Next`, 
    `Legal Barrier At Max Disc`, 
    `hasStaticCheckMaxBreach`, 
    `MaxBends`, 
    `Bends`, 
    `Relative Risk Levels`, 
    `Relative Legal Levels`, 
    `Sum_USDLegalRisky`, 
    `Desk`, 
    `Imnt Id`, 
    `Option Class`, 
    `barType`, 
    `Next Barrier vs Current Spot`, 
    `Barrier at max disc vs Current Spot`, 
    `VCG Accepted`, 
    `Comment`),
    in1.* EXCEPT (`Smart Bending`, 
    `Smart Bending Instrument`, 
    `Imnt Id`, 
    `Final Breach`, 
    `Legal/Risky`, 
    `Abs`, 
    `Check?`, 
    `Inmt ID Accepted`, 
    `Accepted Breach?`, 
    `Final- Breach or Pending QR or <TH`, 
    `Smart Bending Breach`)
  
  FROM Formula_63_0 AS in0
  LEFT JOIN Formula_85_0 AS in1
     ON (in0.`Imnt Id` = in1.`Imnt Id`)

),

AlteryxSelect_84 AS (

  SELECT 
    `Smart Bending Breach` AS `Smart Bending Breach`,
    `Asset Pyramid Name` AS `Asset Pyramid Name`,
    `Asset Spot` AS `Asset Spot`,
    `Legal Barrier Next` AS `Legal Barrier Next`,
    `Legal Barrier At Max Disc` AS `Legal Barrier At Max Disc`,
    hasStaticCheckMaxBreach AS hasStaticCheckMaxBreach,
    MaxBends AS MaxBends,
    Bends AS Bends,
    `Relative Risk Levels` AS `Relative Risk Levels`,
    `Relative Legal Levels` AS `Relative Legal Levels`,
    Sum_USDLegalRisky AS Sum_USDLegalRisky,
    Desk AS Desk,
    `Imnt Id` AS `Imnt Id`,
    `Option Class` AS `Option Class`,
    barType AS barType,
    `Next Barrier vs Current Spot` AS `Next Barrier vs Current Spot`,
    `Barrier at max disc vs Current Spot` AS `Barrier at max disc vs Current Spot`,
    `VCG Accepted` AS `VCG Accepted`,
    Comment AS Comment,
    * EXCEPT (`Smart Bending`, 
    `Smart Bending Instrument`, 
    `Smart Bending Breach`, 
    `Asset Pyramid Name`, 
    `Asset Spot`, 
    `Legal Barrier Next`, 
    `Legal Barrier At Max Disc`, 
    `hasStaticCheckMaxBreach`, 
    `MaxBends`, 
    `Bends`, 
    `Relative Risk Levels`, 
    `Relative Legal Levels`, 
    `Sum_USDLegalRisky`, 
    `Desk`, 
    `Imnt Id`, 
    `Option Class`, 
    `barType`, 
    `Next Barrier vs Current Spot`, 
    `Barrier at max disc vs Current Spot`, 
    `VCG Accepted`, 
    `Comment`)
  
  FROM Join_82_left_UnionLeftOuter AS in0

)

SELECT *

FROM AlteryxSelect_84
