{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH bd_30_Jan_2026__47 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Explain_ELA_with_checks', 'bd_30_Jan_2026__47') }}

),

Filter_89 AS (

  SELECT * 
  
  FROM bd_30_Jan_2026__47 AS in0
  
  WHERE (Metric = 'NCVA')

),

AlteryxSelect_46 AS (

  SELECT 
    CAST(`Official Balance Net` AS DOUBLE) AS `Official Balance Net`,
    * EXCEPT (`Official Balance Net`)
  
  FROM Filter_89 AS in0

),

Summarize_45 AS (

  SELECT 
    SUM(`Official Balance Net`) AS `NCVA Previous month`,
    `Client Name` AS `Client Name`,
    `Client UCN` AS `Client UCN`,
    `Client LE` AS `Client LE`
  
  FROM AlteryxSelect_46 AS in0
  
  GROUP BY 
    `Client Name`, `Client UCN`, `Client LE`

),

Counterparty_CV_29 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Explain_ELA_with_checks', 'Counterparty_CV_29') }}

),

AlteryxSelect_39 AS (

  SELECT 
    `Bank LE` AS `Bank LE`,
    `Net MTM paranthesesOpenTparanthesesClose` AS `Spot PV previous ME`,
    * EXCEPT (`Bank LE`, `Net MTM paranthesesOpenTparanthesesClose`)
  
  FROM Counterparty_CV_29 AS in0

),

Counterparty_CV_28 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Explain_ELA_with_checks', 'Counterparty_CV_28') }}

),

AlteryxSelect_38 AS (

  SELECT 
    `Bank LE` AS `Bank LE`,
    `Net MTM paranthesesOpenTparanthesesClose` AS `Spot PV current ME`,
    * EXCEPT (`Bank LE`, `Net MTM paranthesesOpenTparanthesesClose`)
  
  FROM Counterparty_CV_28 AS in0

),

Join_27_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_38 AS in0
  ANTI JOIN AlteryxSelect_39 AS in1
     ON ((in0.UCNTEXT = in1.UCNTEXT) AND (in0.`Bank LE` = in1.`Bank LE`))

),

ELACalibration2_20 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Explain_ELA_with_checks', 'ELACalibration2_20') }}

),

Filter_66 AS (

  SELECT * 
  
  FROM ELACalibration2_20 AS in0
  
  WHERE (`in scope` = 'Yes')

),

Join_27_inner AS (

  SELECT 
    in0.`Bank LE` AS LE,
    in1.`Trade Count paranthesesOpenTparanthesesClose` AS `Trade count previous ME`,
    in0.Counterparty AS Counterparty,
    in0.`Trade Count paranthesesOpenTparanthesesClose` AS `Trade count current ME`,
    in0.UCN AS UCN,
    in0.`Spot PV current ME` AS `Spot PV current ME`,
    in0.UCNTEXT AS UCNTEXT,
    in0.`NCVA paranthesesOpenTparanthesesClose` AS `NCVA paranthesesOpenTparanthesesClose`,
    in1.`Spot PV previous ME` AS `Spot PV previous ME`
  
  FROM AlteryxSelect_38 AS in0
  INNER JOIN AlteryxSelect_39 AS in1
     ON ((in0.UCNTEXT = in1.UCNTEXT) AND (in0.`Bank LE` = in1.`Bank LE`))

),

Union_40_2 AS (

  SELECT 
    CAST(`NCVA paranthesesOpenTparanthesesClose` AS DOUBLE) AS prophecy_column_5,
    CAST(Counterparty AS string) AS prophecy_column_1,
    CAST(`Trade count previous ME` AS string) AS prophecy_column_6,
    CAST(`Spot PV previous ME` AS DOUBLE) AS prophecy_column_9,
    CAST(UCN AS string) AS prophecy_column_2,
    CAST(UCNTEXT AS string) AS prophecy_column_7,
    CAST(LE AS string) AS prophecy_column_3,
    CAST(`Spot PV current ME` AS DOUBLE) AS prophecy_column_8,
    CAST(`Trade count current ME` AS string) AS prophecy_column_4
  
  FROM Join_27_inner AS in0

),

Join_27_right AS (

  SELECT in0.*
  
  FROM AlteryxSelect_39 AS in0
  ANTI JOIN AlteryxSelect_38 AS in1
     ON ((in1.UCNTEXT = in0.UCNTEXT) AND (in1.`Bank LE` = in0.`Bank LE`))

),

AlteryxSelect_52 AS (

  SELECT 
    Counterparty AS Counterparty,
    UCN AS UCN,
    UCNTEXT AS UCNTEXT,
    `Bank LE` AS LE,
    `Trade Count paranthesesOpenTparanthesesClose` AS `Trade count previous ME`,
    `NCVA paranthesesOpenTparanthesesClose` AS `NCVA paranthesesOpenTparanthesesClose`,
    `Spot PV previous ME` AS `Spot PV previous ME`
  
  FROM Join_27_right AS in0

),

Union_40_1 AS (

  SELECT 
    CAST(`NCVA paranthesesOpenTparanthesesClose` AS DOUBLE) AS prophecy_column_5,
    CAST(Counterparty AS string) AS prophecy_column_1,
    CAST(`Trade count previous ME` AS string) AS prophecy_column_6,
    CAST(`Spot PV previous ME` AS DOUBLE) AS prophecy_column_9,
    CAST(UCN AS string) AS prophecy_column_2,
    CAST(UCNTEXT AS string) AS prophecy_column_7,
    CAST(LE AS string) AS prophecy_column_3
  
  FROM AlteryxSelect_52 AS in0

),

AlteryxSelect_41 AS (

  SELECT 
    Counterparty AS Counterparty,
    UCN AS UCN,
    UCNTEXT AS UCNTEXT,
    `Bank LE` AS LE,
    `Trade Count paranthesesOpenTparanthesesClose` AS `Trade count current ME`,
    `NCVA paranthesesOpenTparanthesesClose` AS `NCVA paranthesesOpenTparanthesesClose`,
    `Spot PV current ME` AS `Spot PV current ME`,
    CAST(NULL AS string) AS `Net MTM current ME`
  
  FROM Join_27_left AS in0

),

Union_40_0 AS (

  SELECT 
    CAST(`NCVA paranthesesOpenTparanthesesClose` AS DOUBLE) AS prophecy_column_5,
    CAST(Counterparty AS string) AS prophecy_column_1,
    CAST(UCN AS string) AS prophecy_column_2,
    CAST(UCNTEXT AS string) AS prophecy_column_7,
    CAST(LE AS string) AS prophecy_column_3,
    CAST(`Spot PV current ME` AS DOUBLE) AS prophecy_column_8,
    CAST(`Trade count current ME` AS string) AS prophecy_column_4
  
  FROM AlteryxSelect_41 AS in0

),

Union_40 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_40_2', 'Union_40_0', 'Union_40_1'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_40_postRename AS (

  SELECT 
    prophecy_column_2 AS UCN,
    prophecy_column_8 AS `Spot PV current ME`,
    prophecy_column_1 AS Counterparty,
    prophecy_column_5 AS `NCVA paranthesesOpenTparanthesesClose`,
    prophecy_column_9 AS `Spot PV previous ME`,
    prophecy_column_3 AS LE,
    prophecy_column_6 AS `Trade count previous ME`,
    prophecy_column_4 AS `Trade count current ME`,
    prophecy_column_7 AS UCNTEXT
  
  FROM Union_40 AS in0

),

AlteryxSelect_51 AS (

  SELECT 
    Counterparty AS `Client Name`,
    * EXCEPT (`NCVA paranthesesOpenTparanthesesClose`, `Counterparty`)
  
  FROM Union_40_postRename AS in0

),

bd_27_Feb_2026__42 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Explain_ELA_with_checks', 'bd_27_Feb_2026__42') }}

),

AlteryxSelect_44 AS (

  SELECT 
    CAST(`Official Balance Net` AS DOUBLE) AS `Official Balance Net`,
    * EXCEPT (`Official Balance Net`)
  
  FROM bd_27_Feb_2026__42 AS in0

),

Summarize_43 AS (

  SELECT 
    SUM(`Official Balance Net`) AS `Official Balance`,
    `Client Name` AS `Client Name`,
    `Client UCN` AS `Client UCN`,
    `Client LE` AS `Client LE`,
    Metric AS Metric
  
  FROM AlteryxSelect_44 AS in0
  
  GROUP BY 
    `Client Name`, `Client UCN`, `Client LE`, Metric

),

Formula_77_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Metric = 'CP CVA')
          THEN 'CP CVA Current Month'
        WHEN (Metric = 'NCVA')
          THEN 'NCVA Current Month'
        ELSE 'Other'
      END
    ) AS string) AS `Current Month Label`,
    *
  
  FROM Summarize_43 AS in0

),

CrossTab_76 AS (

  SELECT *
  
  FROM (
    SELECT 
      `Client Name`,
      `Client UCN`,
      `Client LE`,
      Metric,
      `Current Month Label`,
      `Official Balance`
    
    FROM Formula_77_0 AS in0
  )
  PIVOT (
    SUM(`Official Balance`) AS Sum
    FOR `Current Month Label`
    IN (
      'CP_CVA_Current_Month', 'NCVA_Current_Month'
    )
  )

),

Summarize_85 AS (

  SELECT 
    SUM(CP_CVA_Current_Month) AS `CP CVA Current Month`,
    SUM(NCVA_Current_Month) AS `NCVA Current Month`,
    `Client Name` AS `Client Name`,
    `Client UCN` AS `Client UCN`,
    `Client LE` AS `Client LE`
  
  FROM CrossTab_76 AS in0
  
  GROUP BY 
    `Client Name`, `Client UCN`, `Client LE`

),

Join_48_left AS (

  SELECT in0.*
  
  FROM Summarize_85 AS in0
  ANTI JOIN Summarize_45 AS in1
     ON ((in0.`Client UCN` = in1.`Client UCN`) AND (in0.`Client LE` = in1.`Client LE`))

),

Union_49_0 AS (

  SELECT 
    CAST(`Client Name` AS string) AS prophecy_column_1,
    CAST(`CP CVA Current Month` AS DOUBLE) AS prophecy_column_6,
    CAST(`Client UCN` AS string) AS prophecy_column_2,
    CAST(`Client LE` AS string) AS prophecy_column_3,
    CAST(`NCVA Current Month` AS DOUBLE) AS prophecy_column_4
  
  FROM Join_48_left AS in0

),

ELA_results_202_16 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Explain_ELA_with_checks', 'ELA_results_202_16') }}

),

Filter_18 AS (

  SELECT * 
  
  FROM ELA_results_202_16 AS in0
  
  WHERE (`in scope` = 'Yes')

),

Summarize_17 AS (

  SELECT 
    SUM(ela7) AS `ELA current ME`,
    SUM(pv) AS `PV with KO Current Month`,
    SUM(pv_rf) AS `PV Current Month`,
    LoB AS `Org Level 11 Description`,
    UCN AS UCN,
    LE AS LE,
    `Client Name` AS `Client Name`
  
  FROM Filter_18 AS in0
  
  GROUP BY 
    LoB, UCN, LE, `Client Name`

),

AlteryxSelect_63 AS (

  SELECT 
    CAST(UCN AS string) AS UCN,
    CAST(LE AS string) AS LE,
    * EXCEPT (`UCN`, `LE`)
  
  FROM Summarize_17 AS in0

),

Formula_65_0 AS (

  SELECT 
    CAST((REVERSE((RPAD((REVERSE(UCN)), 12, '0')))) AS string) AS UCN,
    * EXCEPT (`ucn`)
  
  FROM AlteryxSelect_63 AS in0

),

Filter_19 AS (

  SELECT * 
  
  FROM Formula_65_0 AS in0
  
  WHERE (CAST(`Org Level 11 Description` AS string) IN ('CURRENCIES AND EMERGING MARKETS', 'GLOBAL RATES & RATES EXOTICS', 'GLOBAL COMMODITIES'))

),

Summarize_21 AS (

  SELECT 
    SUM(ela7) AS `ELA previous ME`,
    UCN AS UCN,
    LoB AS `Org Level 11 Description`,
    pv AS `PV with KO Previous Month`,
    pv_rf AS `PV Previous Month`,
    LE AS LE,
    `Client Name` AS `Client Name`
  
  FROM Filter_66 AS in0
  
  GROUP BY 
    UCN, LoB, pv, pv_rf, LE, `Client Name`

),

AlteryxSelect_64 AS (

  SELECT 
    CAST(UCN AS string) AS UCN,
    CAST(LE AS string) AS LE,
    * EXCEPT (`UCN`, `LE`)
  
  FROM Summarize_21 AS in0

),

Formula_67_0 AS (

  SELECT 
    CAST((REVERSE((RPAD((REVERSE(UCN)), 12, '0')))) AS string) AS UCN,
    * EXCEPT (`ucn`)
  
  FROM AlteryxSelect_64 AS in0

),

Filter_23 AS (

  SELECT * 
  
  FROM Formula_67_0 AS in0
  
  WHERE (CAST(`Org Level 11 Description` AS string) IN ('CURRENCIES AND EMERGING MARKETS', 'GLOBAL RATES & RATES EXOTICS', 'GLOBAL COMMODITIES'))

),

Join_24_left AS (

  SELECT in0.*
  
  FROM Filter_19 AS in0
  ANTI JOIN Filter_23 AS in1
     ON (
      ((in0.`Org Level 11 Description` = in1.`Org Level 11 Description`) AND (in0.UCN = in1.UCN))
      AND (in0.LE = in1.LE)
    )

),

Union_25_0 AS (

  SELECT 
    CAST(`ELA current ME` AS DOUBLE) AS prophecy_column_5,
    CAST(`Org Level 11 Description` AS string) AS prophecy_column_1,
    CAST(`PV with KO Current Month` AS DOUBLE) AS prophecy_column_6,
    CAST(UCN AS string) AS prophecy_column_2,
    CAST(`PV Current Month` AS DOUBLE) AS prophecy_column_7,
    CAST(LE AS string) AS prophecy_column_3,
    CAST(`Client Name` AS string) AS prophecy_column_4
  
  FROM Join_24_left AS in0

),

Join_24_inner AS (

  SELECT 
    in1.UCN AS Right_UCN,
    in1.`Client Name` AS `Right_Client Name`,
    in0.*,
    in1.* EXCEPT (`Org Level 11 Description`, `UCN`, `LE`, `Client Name`)
  
  FROM Filter_19 AS in0
  INNER JOIN Filter_23 AS in1
     ON (
      ((in0.`Org Level 11 Description` = in1.`Org Level 11 Description`) AND (in0.UCN = in1.UCN))
      AND (in0.LE = in1.LE)
    )

),

Union_25_2 AS (

  SELECT 
    CAST(`ELA current ME` AS DOUBLE) AS prophecy_column_5,
    CAST(`ELA previous ME` AS DOUBLE) AS prophecy_column_10,
    CAST(`Org Level 11 Description` AS string) AS prophecy_column_1,
    CAST(`PV with KO Current Month` AS DOUBLE) AS prophecy_column_6,
    CAST(`Right_Client Name` AS string) AS prophecy_column_9,
    CAST(UCN AS string) AS prophecy_column_2,
    CAST(`PV Previous Month` AS DOUBLE) AS prophecy_column_12,
    CAST(`PV Current Month` AS DOUBLE) AS prophecy_column_7,
    CAST(LE AS string) AS prophecy_column_3,
    CAST(`PV with KO Previous Month` AS DOUBLE) AS prophecy_column_11,
    CAST(Right_UCN AS string) AS prophecy_column_8,
    CAST(`Client Name` AS string) AS prophecy_column_4
  
  FROM Join_24_inner AS in0

),

Join_24_right AS (

  SELECT in0.*
  
  FROM Filter_23 AS in0
  ANTI JOIN Filter_19 AS in1
     ON (
      ((in1.`Org Level 11 Description` = in0.`Org Level 11 Description`) AND (in1.UCN = in0.UCN))
      AND (in1.LE = in0.LE)
    )

),

Union_25_1 AS (

  SELECT 
    CAST(`ELA previous ME` AS DOUBLE) AS prophecy_column_10,
    CAST(`Org Level 11 Description` AS string) AS prophecy_column_1,
    CAST(UCN AS string) AS prophecy_column_2,
    CAST(`PV Previous Month` AS DOUBLE) AS prophecy_column_12,
    CAST(LE AS string) AS prophecy_column_3,
    CAST(`PV with KO Previous Month` AS DOUBLE) AS prophecy_column_11,
    CAST(`Client Name` AS string) AS prophecy_column_4
  
  FROM Join_24_right AS in0

),

Union_25 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_25_2', 'Union_25_0', 'Union_25_1'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_10", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "Double"}, {"name": "prophecy_column_7", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "Double"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}]', 
        '[{"name": "prophecy_column_10", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_25_postRename AS (

  SELECT 
    prophecy_column_2 AS UCN,
    prophecy_column_10 AS `ELA previous ME`,
    prophecy_column_6 AS `PV with KO Current Month`,
    prophecy_column_5 AS `ELA current ME`,
    prophecy_column_1 AS `Org Level 11 Description`,
    prophecy_column_11 AS `PV with KO Previous Month`,
    prophecy_column_7 AS `PV Current Month`,
    prophecy_column_8 AS Right_UCN,
    prophecy_column_12 AS `PV Previous Month`,
    prophecy_column_9 AS `Right_Client Name`,
    prophecy_column_3 AS LE,
    prophecy_column_4 AS `Client Name`
  
  FROM Union_25 AS in0

),

AlteryxSelect_26 AS (

  SELECT 
    `Org Level 11 Description` AS `Org Level 11 Description`,
    UCN AS UCN,
    LE AS LE,
    `Client Name` AS `Client Name`,
    `ELA current ME` AS `ELA current ME`,
    `ELA previous ME` AS `ELA previous ME`,
    `PV with KO Current Month` AS `PV with KO Current Month`,
    `PV Current Month` AS `PV Current Month`,
    `PV with KO Previous Month` AS `PV with KO Previous Month`,
    `PV Previous Month` AS `PV Previous Month`,
    * EXCEPT (`Right_UCN`, 
    `Right_Client Name`, 
    `Org Level 11 Description`, 
    `UCN`, 
    `LE`, 
    `Client Name`, 
    `ELA current ME`, 
    `ELA previous ME`, 
    `PV with KO Current Month`, 
    `PV Current Month`, 
    `PV with KO Previous Month`, 
    `PV Previous Month`)
  
  FROM Union_25_postRename AS in0

),

Join_32_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_26 AS in0
  ANTI JOIN AlteryxSelect_51 AS in1
     ON ((in0.UCN = in1.UCNTEXT) AND (in0.LE = in1.LE))

),

Union_37_0 AS (

  SELECT 
    CAST(`ELA current ME` AS DOUBLE) AS prophecy_column_5,
    CAST(`PV Previous Month` AS DOUBLE) AS prophecy_column_10,
    CAST(`Org Level 11 Description` AS string) AS prophecy_column_1,
    CAST(`ELA previous ME` AS DOUBLE) AS prophecy_column_6,
    CAST(`PV with KO Previous Month` AS DOUBLE) AS prophecy_column_9,
    CAST(UCN AS string) AS prophecy_column_2,
    CAST(`PV with KO Current Month` AS DOUBLE) AS prophecy_column_7,
    CAST(LE AS string) AS prophecy_column_3,
    CAST(`PV Current Month` AS DOUBLE) AS prophecy_column_8,
    CAST(`Client Name` AS string) AS prophecy_column_4
  
  FROM Join_32_left AS in0

),

Join_32_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Client Name`, `UCN`, `LE`, `UCNTEXT`)
  
  FROM AlteryxSelect_26 AS in0
  INNER JOIN AlteryxSelect_51 AS in1
     ON ((in0.UCN = in1.UCNTEXT) AND (in0.LE = in1.LE))

),

Union_37_1 AS (

  SELECT 
    CAST(`ELA current ME` AS DOUBLE) AS prophecy_column_5,
    CAST(`PV Previous Month` AS DOUBLE) AS prophecy_column_10,
    CAST(`Spot PV previous ME` AS DOUBLE) AS prophecy_column_14,
    CAST(`Org Level 11 Description` AS string) AS prophecy_column_1,
    CAST(`ELA previous ME` AS DOUBLE) AS prophecy_column_6,
    CAST(`PV with KO Previous Month` AS DOUBLE) AS prophecy_column_9,
    CAST(`Spot PV current ME` AS DOUBLE) AS prophecy_column_13,
    CAST(UCN AS string) AS prophecy_column_2,
    CAST(`Trade count previous ME` AS string) AS prophecy_column_12,
    CAST(`PV with KO Current Month` AS DOUBLE) AS prophecy_column_7,
    CAST(LE AS string) AS prophecy_column_3,
    CAST(`Trade count current ME` AS string) AS prophecy_column_11,
    CAST(`PV Current Month` AS DOUBLE) AS prophecy_column_8,
    CAST(`Client Name` AS string) AS prophecy_column_4
  
  FROM Join_32_inner AS in0

),

Union_37 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_37_1', 'Union_37_0'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_10", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_13", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_10", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_37_postRename AS (

  SELECT 
    prophecy_column_2 AS UCN,
    prophecy_column_6 AS `ELA previous ME`,
    prophecy_column_7 AS `PV with KO Current Month`,
    prophecy_column_5 AS `ELA current ME`,
    prophecy_column_1 AS `Org Level 11 Description`,
    prophecy_column_9 AS `PV with KO Previous Month`,
    prophecy_column_13 AS `Spot PV current ME`,
    prophecy_column_8 AS `PV Current Month`,
    prophecy_column_10 AS `PV Previous Month`,
    prophecy_column_14 AS `Spot PV previous ME`,
    prophecy_column_3 AS LE,
    prophecy_column_12 AS `Trade count previous ME`,
    prophecy_column_11 AS `Trade count current ME`,
    prophecy_column_4 AS `Client Name`
  
  FROM Union_37 AS in0

),

AlteryxSelect_35 AS (

  SELECT 
    `Client Name` AS `Client Name`,
    UCN AS UCN,
    LE AS LE,
    `ELA current ME` AS `ELA current ME`,
    `ELA previous ME` AS `ELA previous ME`,
    CAST(`Trade count current ME` AS DOUBLE) AS `Trade count current ME`,
    CAST(`Trade count previous ME` AS DOUBLE) AS `Trade count previous ME`,
    `Org Level 11 Description` AS `Org Level 11 Description`,
    `PV with KO Current Month` AS `PV Current Month`,
    `PV with KO Previous Month` AS `PV Previous month`,
    `Spot PV current ME` AS `Spot PV current ME`,
    `Spot PV previous ME` AS `Spot PV previous ME`,
    CAST(NULL AS string) AS UCNTEXT,
    CAST(NULL AS string) AS `Exposure Metric`,
    CAST(NULL AS string) AS `Net MTM current ME`,
    * EXCEPT (`PV Current Month`, 
    `PV Previous month`, 
    `Client Name`, 
    `UCN`, 
    `LE`, 
    `ELA current ME`, 
    `ELA previous ME`, 
    `Trade count current ME`, 
    `Trade count previous ME`, 
    `Org Level 11 Description`, 
    `Spot PV current ME`, 
    `Spot PV previous ME`, 
    `PV with KO Current Month`, 
    `PV with KO Previous Month`)
  
  FROM Union_37_postRename AS in0

),

Join_48_inner AS (

  SELECT 
    in0.`Client Name` AS `Client Name`,
    in0.`Client UCN` AS `Client UCN`,
    in0.`Client LE` AS `Client LE`,
    in0.`NCVA Current Month` AS `NCVA Current Month`,
    in1.`NCVA Previous month` AS `NCVA Previous month`,
    in0.`CP CVA Current Month` AS `CP CVA Current Month`,
    in0.* EXCEPT (`Client Name`, `Client UCN`, `Client LE`, `NCVA Current Month`, `CP CVA Current Month`),
    in1.* EXCEPT (`Client Name`, `Client UCN`, `Client LE`, `NCVA Previous month`)
  
  FROM Summarize_85 AS in0
  INNER JOIN Summarize_45 AS in1
     ON ((in0.`Client UCN` = in1.`Client UCN`) AND (in0.`Client LE` = in1.`Client LE`))

),

Union_49_2 AS (

  SELECT 
    CAST(`NCVA Previous month` AS DOUBLE) AS prophecy_column_5,
    CAST(`Client Name` AS string) AS prophecy_column_1,
    CAST(`CP CVA Current Month` AS DOUBLE) AS prophecy_column_6,
    CAST(`Client UCN` AS string) AS prophecy_column_2,
    CAST(`Client LE` AS string) AS prophecy_column_3,
    CAST(`NCVA Current Month` AS DOUBLE) AS prophecy_column_4
  
  FROM Join_48_inner AS in0

),

Join_48_right AS (

  SELECT in0.*
  
  FROM Summarize_45 AS in0
  ANTI JOIN Summarize_85 AS in1
     ON ((in1.`Client UCN` = in0.`Client UCN`) AND (in1.`Client LE` = in0.`Client LE`))

),

Union_49_1 AS (

  SELECT 
    CAST(`Client Name` AS string) AS prophecy_column_1,
    CAST(`Client UCN` AS string) AS prophecy_column_2,
    CAST(`Client LE` AS string) AS prophecy_column_3,
    CAST(`NCVA Previous month` AS DOUBLE) AS prophecy_column_5
  
  FROM Join_48_right AS in0

),

Union_49 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_49_2', 'Union_49_0', 'Union_49_1'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_49_postRename AS (

  SELECT 
    prophecy_column_6 AS `CP CVA Current Month`,
    prophecy_column_5 AS `NCVA Previous month`,
    prophecy_column_4 AS `NCVA Current Month`,
    prophecy_column_3 AS `Client LE`,
    prophecy_column_1 AS `Client Name`,
    prophecy_column_2 AS `Client UCN`
  
  FROM Union_49 AS in0

),

Join_50_inner AS (

  SELECT 
    in0.* EXCEPT (`UCNTEXT`),
    in1.* EXCEPT (`Client Name`, `Client UCN`, `Client LE`)
  
  FROM AlteryxSelect_35 AS in0
  INNER JOIN Union_49_postRename AS in1
     ON ((in0.UCN = in1.`Client UCN`) AND (in0.LE = in1.`Client LE`))

),

bd_27_Feb_2026__53 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Explain_ELA_with_checks', 'bd_27_Feb_2026__53') }}

),

AlteryxSelect_55 AS (

  SELECT 
    CAST(`Balance Approach MTD PnL paranthesesOpenBSparanthesesClose` AS DOUBLE) AS `Balance Approach MTD PnL paranthesesOpenBSparanthesesClose`,
    CAST(`PnL MTD per Bucket Total PnL paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket Total PnL paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Mktmove paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Mktmove paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Mkt Reversal paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Mkt Reversal paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Mkt Predicts paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Mkt Predicts paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Mkt ResidualparanthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Mkt ResidualparanthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Mktcreditcorr paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL mktcreditcorr paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Mtdtimedecay paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Mtdtimedecay paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Datesliding paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Datesliding paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Regressionradar paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Regressionradar paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Mktmodel paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Mktmodel paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Netting paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Netting paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Collateral paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Collateral paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL DD Deal Activity paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL DD Deal Activity paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Creditspread paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Creditspread paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Fundingspread paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Fundingspread paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Timedecay paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Timedecay paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Credit Timedecay paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Credit Timedecay paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Funding Timedecay paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Funding Timedecay paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL CSA Timedecay paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL CSA Timedecay paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Base Timedecay paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Base Timedecay paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Modelcross paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Modelcross paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Other Adj paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Other Adj paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Other Def CP CB paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Other Def CP CB paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL other Def CP IB paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL other Def CP IB paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Credit Risk Fund paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Credit Risk Fund paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Def Deriv Fund paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Def Deriv Fund paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Earnings On Capital paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Earnings On Capital paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL IB Treasury Charge paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL IB Treasury Charge paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Mkt Model Reserve paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Mkt Model Reserve paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Mkt Risk Funding paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Mkt Risk Funding paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL SC Trups paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL SC Trups paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Sva Liq Excess paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Sva Liq Excess paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Sva Ola Top Up paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Sva Ola Top Up paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Vcg Credit Clean paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Vcg Credit Clean paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Vcg Credit Reserve paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Vcg Credit Reserve paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Vcg Mkt Clean paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Vcg Mkt Clean paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Vcg Mkt Reserve paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Vcg Mkt Reserve paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Vcg Model Reserve paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Vcg Model Reserve paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL PB System Residual paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL PB System Residual paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Invoiced DVA DA paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Invoiced DVA DA paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Invoiced DA paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Invoiced DA paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Pending DA paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Pending DA paranthesesOpenMTDparanthesesClose`,
    CAST(`PnL MTD per Bucket PnL Pending DVA DA paranthesesOpenMTDparanthesesClose` AS DOUBLE) AS `PnL MTD per Bucket PnL Pending DVA DA paranthesesOpenMTDparanthesesClose`,
    CAST(NULL AS string) AS `PnL WTD per Bucket Total PnL paranthesesOpenWTDparanthesesClose`,
    * EXCEPT (`Balance Approach MTD PnL paranthesesOpenBSparanthesesClose`, 
    `PnL MTD per Bucket Total PnL paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Mktmove paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Mkt Reversal paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Mkt Predicts paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Mkt ResidualparanthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL mktcreditcorr paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Mtdtimedecay paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Datesliding paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Regressionradar paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Mktmodel paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Netting paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Collateral paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL DD Deal Activity paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Creditspread paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Fundingspread paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Timedecay paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Credit Timedecay paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Funding Timedecay paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL CSA Timedecay paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Base Timedecay paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Modelcross paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Other Adj paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Other Def CP CB paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL other Def CP IB paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Credit Risk Fund paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Def Deriv Fund paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Earnings On Capital paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL IB Treasury Charge paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Mkt Model Reserve paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Mkt Risk Funding paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL SC Trups paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Sva Liq Excess paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Sva Ola Top Up paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Vcg Credit Clean paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Vcg Credit Reserve paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Vcg Mkt Clean paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Vcg Mkt Reserve paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Vcg Model Reserve paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL PB System Residual paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Invoiced DVA DA paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Invoiced DA paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Pending DA paranthesesOpenMTDparanthesesClose`, 
    `PnL MTD per Bucket PnL Pending DVA DA paranthesesOpenMTDparanthesesClose`)
  
  FROM bd_27_Feb_2026__53 AS in0

),

Summarize_54 AS (

  SELECT 
    SUM(`PnL MTD per Bucket Total PnL paranthesesOpenMTDparanthesesClose`) AS `Sum_PnL MTD per Bucket Total PnL paranthesesOpenMTDparanthesesClose`,
    SUM(`PnL MTD per Bucket PnL Mkt Predicts paranthesesOpenMTDparanthesesClose`) AS `Sum_PnL MTD per Bucket PnL Mkt Predicts paranthesesOpenMTDparanthesesClose`,
    SUM(`PnL MTD per Bucket PnL mktcreditcorr paranthesesOpenMTDparanthesesClose`) AS `Sum_PnL MTD per Bucket PnL mktcreditcorr paranthesesOpenMTDparanthesesClose`,
    SUM(`PnL MTD per Bucket PnL Mtdtimedecay paranthesesOpenMTDparanthesesClose`) AS `Sum_PnL MTD per Bucket PnL Mtdtimedecay paranthesesOpenMTDparanthesesClose`,
    SUM(`PnL MTD per Bucket PnL Mktmodel paranthesesOpenMTDparanthesesClose`) AS `Sum_PnL MTD per Bucket PnL Mktmodel paranthesesOpenMTDparanthesesClose`,
    SUM(`PnL MTD per Bucket PnL Invoiced DA paranthesesOpenMTDparanthesesClose`) AS `Sum_PnL MTD per Bucket PnL Invoiced DA paranthesesOpenMTDparanthesesClose`,
    `Client Name` AS `Client Name`,
    `Client UCN` AS `Client UCN`,
    `Client LE` AS `Client LE`
  
  FROM AlteryxSelect_55 AS in0
  
  GROUP BY 
    `Client Name`, `Client UCN`, `Client LE`

),

Formula_56_0 AS (

  SELECT 
    CAST((
      (
        (
          (
            `Sum_PnL MTD per Bucket PnL Mkt Predicts paranthesesOpenMTDparanthesesClose`
            + `Sum_PnL MTD per Bucket PnL mktcreditcorr paranthesesOpenMTDparanthesesClose`
          )
          + `Sum_PnL MTD per Bucket PnL Mtdtimedecay paranthesesOpenMTDparanthesesClose`
        )
        + `Sum_PnL MTD per Bucket PnL Mktmodel paranthesesOpenMTDparanthesesClose`
      )
      * -1
    ) AS DOUBLE) AS `NCVA Mkt move MoM`,
    *
  
  FROM Summarize_54 AS in0

),

AlteryxSelect_57 AS (

  SELECT 
    `Sum_PnL MTD per Bucket PnL Invoiced DA paranthesesOpenMTDparanthesesClose` AS `NCVA deal activity`,
    * EXCEPT (`Sum_PnL MTD per Bucket PnL Mkt Predicts paranthesesOpenMTDparanthesesClose`, 
    `Sum_PnL MTD per Bucket PnL mktcreditcorr paranthesesOpenMTDparanthesesClose`, 
    `Sum_PnL MTD per Bucket PnL Mtdtimedecay paranthesesOpenMTDparanthesesClose`, 
    `Sum_PnL MTD per Bucket PnL Mktmodel paranthesesOpenMTDparanthesesClose`, 
    `Sum_PnL MTD per Bucket PnL Invoiced DA paranthesesOpenMTDparanthesesClose`)
  
  FROM Formula_56_0 AS in0

),

Join_59_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Client Name`, `Client UCN`, `Client LE`)
  
  FROM Join_50_inner AS in0
  INNER JOIN AlteryxSelect_57 AS in1
     ON ((in0.UCN = in1.`Client UCN`) AND (in0.LE = in1.`Client LE`))

)

SELECT *

FROM Join_59_inner
