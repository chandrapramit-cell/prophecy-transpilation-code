{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH BarReport_APAC__1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Barrier_Bend_Monitoring_1_', 'BarReport_APAC__1') }}

),

Summarize_29 AS (

  SELECT 
    MAX((
      CASE
        WHEN (hasStaticCheckMaxBreach = 'TRUE')
          THEN USDLegalRisky
        ELSE NULL
      END
    )) AS Max_USDLegalRisky,
    `Imnt Id` AS `Imnt Id`,
    `Imnt Name` AS `Imnt Name`,
    `Option Class` AS `Option Class`,
    MultiAssetMonitoringType AS MultiAssetMonitoringType
  
  FROM BarReport_APAC__1 AS in0
  
  GROUP BY 
    `Imnt Id`, `Imnt Name`, `Option Class`, MultiAssetMonitoringType

),

AlteryxSelect_4 AS (

  SELECT `Imnt Id` AS `Imnt Id`
  
  FROM BarReport_APAC__1 AS in0

),

Unique_3 AS (

  SELECT * 
  
  FROM AlteryxSelect_4 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `Imnt Id` ORDER BY `Imnt Id`) = 1

),

Join_32_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Imnt Id` = in1.`Imnt Id`)
          THEN in1.`Imnt Id`
        ELSE NULL
      END
    ) AS `Imnt Id hasStaticCheckMaxBreach`,
    in0.*,
    in1.* EXCEPT (`Imnt Name`, `Option Class`, `MultiAssetMonitoringType`, `Max_USDLegalRisky`, `Imnt Id`)
  
  FROM Unique_3 AS in0
  LEFT JOIN Summarize_29 AS in1
     ON (in0.`Imnt Id` = in1.`Imnt Id`)

),

Summarize_37 AS (

  SELECT 
    MAX(USDLegalRisky) AS Max_USDLegalRisky,
    `Imnt Id` AS `Imnt Id`,
    `Imnt Name` AS `Imnt Name`,
    `Option Class` AS `Option Class`,
    MultiAssetMonitoringType AS MultiAssetMonitoringType
  
  FROM BarReport_APAC__1 AS in0
  
  GROUP BY 
    `Imnt Id`, `Imnt Name`, `Option Class`, MultiAssetMonitoringType

),

Unique_39 AS (

  SELECT * 
  
  FROM Summarize_37 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `Imnt Id` ORDER BY `Imnt Id`) = 1

),

Join_38_inner AS (

  SELECT 
    in0.`Imnt Id` AS `Imnt Id`,
    in0.`Imnt Id hasStaticCheckMaxBreach` AS `Imnt Id hasStaticCheckMaxBreach`,
    in1.`Imnt Id` AS `Imnt Id All`,
    in1.Max_USDLegalRisky AS `Legal/Risky`
  
  FROM Join_32_left_UnionLeftOuter AS in0
  INNER JOIN Unique_39 AS in1
     ON (in0.`Imnt Id` = in1.`Imnt Id`)

),

Formula_34_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((`Imnt Id hasStaticCheckMaxBreach` IS NULL) AS BOOLEAN)
          THEN 'FALSE'
        ELSE 'TRUE'
      END
    ) AS string) AS `Final Breach`,
    CAST(ABS(CAST(`Legal/Risky` AS DECIMAL (19, 9))) AS DOUBLE) AS Abs,
    *
  
  FROM Join_38_inner AS in0

),

Formula_34_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Abs > 50000)
          THEN 'TRUE'
        ELSE 'FALSE'
      END
    ) AS string) AS `Check?`,
    *
  
  FROM Formula_34_0 AS in0

),

AlteryxSelect_40 AS (

  SELECT 
    `Imnt Id` AS `Imnt Id`,
    `Final Breach` AS `Final Breach`,
    `Legal/Risky` AS `Legal/Risky`,
    Abs AS Abs,
    `Check?` AS `Check?`,
    * EXCEPT (`Imnt Id hasStaticCheckMaxBreach`, `Imnt Id All`, `Imnt Id`, `Final Breach`, `Legal/Risky`, `Abs`, `Check?`)
  
  FROM Formula_34_1 AS in0

),

TextInput_9 AS (

  SELECT * 
  
  FROM {{ ref('seed_Barrier_Bend_Monitoring_1__9')}}

),

TextInput_9_cast AS (

  SELECT 
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS variableDate,
    CAST(Days AS INTEGER) AS Days
  
  FROM TextInput_9 AS in0

),

AlteryxSelect_15 AS (

  SELECT 
    variableDate AS variableDate,
    CAST(Days AS string) AS Days
  
  FROM TextInput_9_cast AS in0

),

BarrierBendingM_5 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Barrier_Bend_Monitoring_1_', 'BarrierBendingM_5') }}

),

AlteryxSelect_6 AS (

  SELECT 
    Desk AS Desk,
    CAST(`Inmt ID` AS string) AS `Inmt ID`,
    `Option Class` AS `Option Class`,
    `Bar Type` AS `Bar Type`,
    `Next Barrier vs Current Spot` AS `Next Barrier vs Current Spot`,
    `Barrier at max disc vs Current Spot` AS `Barrier at max disc vs Current Spot`,
    `VCG Accepted` AS `VCG Accepted`,
    Comment AS Comment,
    `Review Date` AS `Review Date`
  
  FROM BarrierBendingM_5 AS in0

),

AppendFields_24 AS (

  SELECT 
    in0.variableDate AS Source_Date,
    in0.Days AS Source_Days,
    in0.* EXCEPT (`variableDate`, `Days`),
    in1.*
  
  FROM AlteryxSelect_15 AS in0
  INNER JOIN AlteryxSelect_6 AS in1
     ON TRUE

),

Filter_25_to_Filter_26 AS (

  SELECT * 
  
  FROM AppendFields_24 AS in0
  
  WHERE ((Source_Days = '30') AND (NOT(`Inmt ID` IS NULL)))

),

AlteryxSelect_21 AS (

  SELECT 
    `Inmt ID` AS `Inmt ID`,
    * EXCEPT (`Inmt ID`)
  
  FROM Filter_25_to_Filter_26 AS in0

),

Join_20_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Inmt ID` = in1.`Imnt Id`)
          THEN in1.`Imnt Id`
        ELSE NULL
      END
    ) AS `Imnt Id Current ME`,
    in0.*,
    in1.* EXCEPT (`Imnt Id`)
  
  FROM AlteryxSelect_21 AS in0
  LEFT JOIN Unique_3 AS in1
     ON (in0.`Inmt ID` = in1.`Imnt Id`)

),

Formula_27_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((`Imnt Id Current ME` IS NULL) AS BOOLEAN)
          THEN 'Expired'
        ELSE `Imnt Id Current ME`
      END
    ) AS string) AS `Current Month`,
    CAST(CASE
      WHEN (CAST(datediff(to_date(Source_Date), to_date(`Review Date`)) AS INT) > Source_Days)
        THEN 'Y'
      ELSE 'N'
    END AS STRING) AS `Age?`,
    *
  
  FROM Join_20_left_UnionLeftOuter AS in0

),

Summarize_42 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN (`Age?` = 'N')
            THEN (
              CASE
                WHEN ((`Inmt ID` IS NULL) OR (CAST(`Inmt ID` AS string) = ''))
                  THEN NULL
                ELSE 1
              END
            )
          ELSE NULL
        END
      )) AS `Count`,
    `Inmt ID` AS `Inmt ID`
  
  FROM Formula_27_0 AS in0
  
  GROUP BY `Inmt ID`

),

Join_41_left_UnionLeftOuter AS (

  SELECT 
    in0.`Imnt Id` AS `Imnt Id`,
    in0.`Final Breach` AS `Final Breach`,
    in0.`Legal/Risky` AS `Legal/Risky`,
    in0.Abs AS Abs,
    in0.`Check?` AS `Check?`,
    (
      CASE
        WHEN (in0.`Imnt Id` = in1.`Inmt ID`)
          THEN in1.`Inmt ID`
        ELSE NULL
      END
    ) AS `Inmt ID Accepted`,
    in0.* EXCEPT (`Imnt Id`, `Final Breach`, `Legal/Risky`, `Abs`, `Check?`),
    in1.* EXCEPT (`Count`, `Inmt ID`)
  
  FROM AlteryxSelect_40 AS in0
  LEFT JOIN Summarize_42 AS in1
     ON (in0.`Imnt Id` = in1.`Inmt ID`)

),

Formula_46_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((`Inmt ID Accepted` IS NULL) AS BOOLEAN)
          THEN 'FALSE'
        ELSE 'TRUE'
      END
    ) AS string) AS `Accepted Breach?`,
    *
  
  FROM Join_41_left_UnionLeftOuter AS in0

),

Formula_46_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`Check?` = 'FALSE')
          THEN 'No Breach'
        WHEN (`Final Breach` = 'FALSE')
          THEN 'No breach'
        WHEN (`Accepted Breach?` = 'TRUE')
          THEN 'Breach Accepted'
        ELSE 'Breach'
      END
    ) AS string) AS `Final- Breach or Pending QR or <TH`,
    *
  
  FROM Formula_46_0 AS in0

),

Formula_72_3 AS (

  SELECT *
  
  FROM {{ ref('Barrier_Bend_Monitoring_1___Formula_72_3')}}

),

Unique_104 AS (

  SELECT * 
  
  FROM Formula_72_3 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `Inmt Id` ORDER BY `Inmt Id`) = 1

),

Join_77_left_UnionLeftOuter AS (

  SELECT 
    in0.`Accepted Breach?` AS `Accepted Breach?`,
    in0.`Legal/Risky` AS `Legal/Risky`,
    in0.`Imnt Id` AS `Imnt Id`,
    in0.`Final- Breach or Pending QR or <TH` AS `Final- Breach or Pending QR or <TH`,
    in0.`Final Breach` AS `Final Breach`,
    in0.`Inmt ID Accepted` AS `Inmt ID Accepted`,
    in1.`Smart Bending` AS `Smart Bending`,
    in0.`Check?` AS `Check?`,
    in0.Abs AS Abs,
    (
      CASE
        WHEN (in0.`Imnt Id` = in1.`Inmt Id`)
          THEN in1.`Inmt Id`
        ELSE NULL
      END
    ) AS `Smart Bending Instrument`
  
  FROM Formula_46_1 AS in0
  LEFT JOIN Unique_104 AS in1
     ON (in0.`Imnt Id` = in1.`Inmt Id`)

),

Formula_85_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((`Smart Bending` IS NULL) AS BOOLEAN)
          THEN 'FALSE'
        ELSE 'TRUE'
      END
    ) AS string) AS `Smart Bending Breach`,
    *
  
  FROM Join_77_left_UnionLeftOuter AS in0

)

SELECT *

FROM Formula_85_0
