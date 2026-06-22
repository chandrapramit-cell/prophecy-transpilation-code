{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_59_inner AS (

  SELECT *
  
  FROM {{ ref('Explain_ELA_with_checks__Join_59_inner')}}

),

Cleanse_80 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_59_inner'], 
      [
        { "name": "Client Name", "dataType": "String" }, 
        { "name": "UCN", "dataType": "String" }, 
        { "name": "LE", "dataType": "String" }, 
        { "name": "ELA current ME", "dataType": "Double" }, 
        { "name": "ELA previous ME", "dataType": "Double" }, 
        { "name": "Trade count current ME", "dataType": "Double" }, 
        { "name": "Trade count previous ME", "dataType": "Double" }, 
        { "name": "Org Level 11 Description", "dataType": "String" }, 
        { "name": "PV Current Month", "dataType": "Double" }, 
        { "name": "PV Previous month", "dataType": "Double" }, 
        { "name": "Spot PV current ME", "dataType": "Double" }, 
        { "name": "Spot PV previous ME", "dataType": "Double" }, 
        { "name": "CP CVA Current Month", "dataType": "Double" }, 
        { "name": "NCVA Previous month", "dataType": "Double" }, 
        { "name": "NCVA Current Month", "dataType": "Double" }, 
        { "name": "NCVA deal activity", "dataType": "Double" }, 
        { "name": "NCVA Mkt move MoM", "dataType": "Double" }, 
        { "name": "Sum_PnL MTD per Bucket Total PnL (MTD)", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'ELA current ME', 
        'ELA previous ME', 
        'Spot PV current ME', 
        'Spot PV previous ME', 
        'NCVA Previous month', 
        'Sum_PnL MTD per Bucket Total PnL (MTD)', 
        'NCVA Mkt move MoM'
      ], 
      false, 
      '', 
      true, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Formula_79_0 AS (

  SELECT 
    CAST((`ELA current ME` - `ELA previous ME`) AS DOUBLE) AS `ELA MoM`,
    CAST((`NCVA Current Month` - `NCVA Previous month`) AS DOUBLE) AS `NCVA MoM`,
    *
  
  FROM Cleanse_80 AS in0

),

Formula_79_1 AS (

  SELECT 
    CAST((`NCVA deal activity` / `NCVA MoM`) AS DOUBLE) AS `New trade pnl % of NCVA`,
    CAST(((`NCVA MoM` - `NCVA Mkt move MoM`) - `NCVA deal activity`) AS DOUBLE) AS `NCVA other move`,
    CAST((`NCVA Mkt move MoM` / `NCVA MoM`) AS DOUBLE) AS `market move pnl % of NCVA`,
    *
  
  FROM Formula_79_0 AS in0

),

Formula_79_2 AS (

  SELECT 
    CAST((`NCVA other move` / `NCVA MoM`) AS DOUBLE) AS `NCVA other moves %`,
    CAST(ABS((`ELA current ME` - `ELA previous ME`)) AS DOUBLE) AS `Gross ELA MOM`,
    *
  
  FROM Formula_79_1 AS in0

),

Filter_84 AS (

  SELECT * 
  
  FROM Formula_79_2 AS in0
  
  WHERE (`Org Level 11 Description` = 'GLOBAL RATES & RATES EXOTICS')

),

AlteryxSelect_81 AS (

  SELECT 
    `Org Level 11 Description` AS `Org Level 11 Description`,
    `Client Name` AS `Client Name`,
    UCN AS UCN,
    LE AS LE,
    `ELA current ME` AS `ELA current ME`,
    `ELA previous ME` AS `ELA previous ME`,
    `Trade count current ME` AS `Trade count current ME`,
    `Trade count previous ME` AS `Trade count previous ME`,
    `Spot PV current ME` AS `Spot PV current ME`,
    `Spot PV previous ME` AS `Spot PV previous ME`,
    `NCVA Current Month` AS `NCVA Current Month`,
    `NCVA Previous month` AS `NCVA Previous month`,
    `ELA MoM` AS `ELA MoM`,
    `Gross ELA MOM` AS `Gross ELA MOM`,
    `NCVA MoM` AS `NCVA MoM`,
    `NCVA deal activity` AS `NCVA deal activity`,
    `NCVA Mkt move MoM` AS `NCVA Mkt move MoM`,
    `NCVA other move` AS `NCVA other move`,
    `New trade pnl % of NCVA` AS `New trade pnl % of NCVA`,
    `NCVA other moves %` AS `NCVA other moves %`,
    `market move pnl % of NCVA` AS `market move pnl % of NCVA`,
    * EXCEPT (`PV Current Month`, 
    `PV Previous month`, 
    `Sum_PnL MTD per Bucket Total PnL (MTD)`, 
    `CP CVA Current Month`, 
    `Org Level 11 Description`, 
    `Client Name`, 
    `UCN`, 
    `LE`, 
    `ELA current ME`, 
    `ELA previous ME`, 
    `Trade count current ME`, 
    `Trade count previous ME`, 
    `Spot PV current ME`, 
    `Spot PV previous ME`, 
    `NCVA Current Month`, 
    `NCVA Previous month`, 
    `ELA MoM`, 
    `Gross ELA MOM`, 
    `NCVA MoM`, 
    `NCVA deal activity`, 
    `NCVA Mkt move MoM`, 
    `NCVA other move`, 
    `New trade pnl % of NCVA`, 
    `NCVA other moves %`, 
    `market move pnl % of NCVA`)
  
  FROM Filter_84 AS in0

)

SELECT *

FROM AlteryxSelect_81
