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

Filter_86_to_Filter_90 AS (

  SELECT * 
  
  FROM Formula_60_2 AS in0
  
  WHERE (
          (CAST(`Org Level 11 Description` AS string) IN ('CURRENCIES AND EMERGING MARKETS', 'GLOBAL RATES & RATES EXOTICS', 'GLOBAL COMMODITIES'))
          AND (`CPCVAslashNCVA Check` >= 1)
        )

),

AlteryxSelect_88 AS (

  SELECT 
    `Org Level 11 Description` AS `Org Level 11 Description`,
    `Client Name` AS `Client Name`,
    UCN AS UCN,
    LE AS LE,
    `ELA current ME` AS `ELA current ME`,
    `NCVA Current Month` AS `NCVA Current Month`,
    `CP CVA Current Month` AS `CP CVA Current Month`,
    `CPCVAslashNCVA Check` AS `CP CVAslash NCVA percent`,
    * EXCEPT (`ELA previous ME`, 
    `Trade count current ME`, 
    `Trade count previous ME`, 
    `PV Current Month`, 
    `PV Previous month`, 
    `NCVA Previous month`, 
    `ELA MoM`, 
    `Gross ELA MOM`, 
    `NCVA MoM`, 
    `Sum_PnL MTD per Bucket Total PnL paranthesesOpenMTDparanthesesClose`, 
    `NCVA deal activity`, 
    `NCVA Mkt move MoM`, 
    `NCVA other move`, 
    `New trade pnl percent of NCVA`, 
    `NCVA other moves percent`, 
    `market move pnl percent of NCVA`, 
    `Spot PV current ME`, 
    `Spot PV previous ME`, 
    `Org Level 11 Description`, 
    `Client Name`, 
    `UCN`, 
    `LE`, 
    `ELA current ME`, 
    `NCVA Current Month`, 
    `CP CVA Current Month`, 
    `CPCVAslashNCVA Check`)
  
  FROM Filter_86_to_Filter_90 AS in0

)

SELECT *

FROM AlteryxSelect_88
