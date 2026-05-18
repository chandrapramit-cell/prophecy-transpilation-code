{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_60_2 AS (

  SELECT *
  
  FROM {{ ref('Explain_ELA_with_checks__Formula_60_2')}}

),

Filter_70 AS (

  SELECT * 
  
  FROM Formula_60_2 AS in0
  
  WHERE (`Org Level 11 Description` = 'CURRENCIES AND EMERGING MARKETS')

),

AlteryxSelect_62 AS (

  SELECT 
    `Org Level 11 Description` AS `Org Level 11 Description`,
    `Client Name` AS `Client Name`,
    UCN AS UCN,
    LE AS LE,
    `ELA current ME` AS `ELA current ME`,
    `ELA previous ME` AS `ELA previous ME`,
    `Trade count current ME` AS `Trade count current ME`,
    `Trade count previous ME` AS `Trade count previous ME`,
    `PV Current Month` AS `PV Current Month`,
    `PV Previous month` AS `PV Previous month`,
    `NCVA Current Month` AS `NCVA Current Month`,
    `NCVA Previous month` AS `NCVA Previous month`,
    `ELA MoM` AS `ELA MoM`,
    `Gross ELA MOM` AS `Gross ELA MOM`,
    `NCVA MoM` AS `NCVA MoM`,
    `NCVA deal activity` AS `NCVA deal activity`,
    `NCVA Mkt move MoM` AS `NCVA Mkt move MoM`,
    `NCVA other move` AS `NCVA other move`,
    `New trade pnl percent of NCVA` AS `New trade pnl percent of NCVA`,
    `NCVA other moves percent` AS `NCVA other moves percent`,
    `market move pnl percent of NCVA` AS `market move pnl percent of NCVA`,
    CAST(NULL AS string) AS UCNTEXT,
    * EXCEPT (`Sum_PnL MTD per Bucket Total PnL paranthesesOpenMTDparanthesesClose`, 
    `CP CVA Current Month`, 
    `Spot PV current ME`, 
    `Spot PV previous ME`, 
    `CPCVAslashNCVA Check`, 
    `Org Level 11 Description`, 
    `Client Name`, 
    `UCN`, 
    `LE`, 
    `ELA current ME`, 
    `ELA previous ME`, 
    `Trade count current ME`, 
    `Trade count previous ME`, 
    `PV Current Month`, 
    `PV Previous month`, 
    `NCVA Current Month`, 
    `NCVA Previous month`, 
    `ELA MoM`, 
    `Gross ELA MOM`, 
    `NCVA MoM`, 
    `NCVA deal activity`, 
    `NCVA Mkt move MoM`, 
    `NCVA other move`, 
    `New trade pnl percent of NCVA`, 
    `NCVA other moves percent`, 
    `market move pnl percent of NCVA`)
  
  FROM Filter_70 AS in0

)

SELECT *

FROM AlteryxSelect_62
