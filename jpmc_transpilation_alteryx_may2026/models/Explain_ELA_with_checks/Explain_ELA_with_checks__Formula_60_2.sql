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

Cleanse_61 AS (

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
        { "name": "Exposure Metric", "dataType": "String" }, 
        { "name": "Net MTM current ME", "dataType": "String" }, 
        { "name": "CP CVA Current Month", "dataType": "Double" }, 
        { "name": "NCVA Previous month", "dataType": "Double" }, 
        { "name": "NCVA Current Month", "dataType": "Double" }, 
        { "name": "NCVA deal activity", "dataType": "Double" }, 
        { "name": "NCVA Mkt move MoM", "dataType": "Double" }, 
        { "name": "Sum_PnL MTD per Bucket Total PnL paranthesesOpenMTDparanthesesClose", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'ELA current ME', 
        'ELA previous ME', 
        'PV Current Month', 
        'PV Previous month', 
        'NCVA Previous month', 
        'Sum_PnL MTD per Bucket Total PnL paranthesesOpenMTDparanthesesClose', 
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

Formula_60_0 AS (

  SELECT 
    CAST((`ELA current ME` - `ELA previous ME`) AS DOUBLE) AS `ELA MoM`,
    CAST((`NCVA Current Month` - `NCVA Previous month`) AS DOUBLE) AS `NCVA MoM`,
    *
  
  FROM Cleanse_61 AS in0

),

Formula_60_1 AS (

  SELECT 
    CAST((`NCVA deal activity` / `NCVA MoM`) AS DOUBLE) AS `New trade pnl percent of NCVA`,
    CAST(((`NCVA MoM` - `NCVA Mkt move MoM`) - `NCVA deal activity`) AS DOUBLE) AS `NCVA other move`,
    CAST((`NCVA Mkt move MoM` / `NCVA MoM`) AS DOUBLE) AS `market move pnl percent of NCVA`,
    *
  
  FROM Formula_60_0 AS in0

),

Formula_60_2 AS (

  SELECT 
    CAST((`NCVA other move` / `NCVA MoM`) AS DOUBLE) AS `NCVA other moves percent`,
    CAST(ABS((`ELA current ME` - `ELA previous ME`)) AS DOUBLE) AS `Gross ELA MOM`,
    CAST((`CP CVA Current Month` / `NCVA Current Month`) AS DOUBLE) AS `CPCVAslashNCVA Check`,
    *
  
  FROM Formula_60_1 AS in0

)

SELECT *

FROM Formula_60_2
