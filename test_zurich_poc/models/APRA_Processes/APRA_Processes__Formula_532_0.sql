{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_446_0 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_446_0')}}

),

Formula_473_0 AS (

  SELECT 
    CAST(1 AS INTEGER) AS `Line Number`,
    *
  
  FROM Formula_446_0 AS in0

),

AlteryxSelect_472 AS (

  SELECT 
    Period AS Period,
    Company AS Company,
    Reinsurer AS Reinsurer,
    `RI Type IPS` AS `RI Type IPS`,
    `RI Type` AS `RI Type`,
    Branch AS Branch,
    `Account Type` AS `Account Type`,
    `Accident Year` AS `Accident Year`,
    `Product Code` AS `Product Code`,
    `Premium Class` AS `Premium Class`,
    `Line Number` AS `Line Number`,
    TZAC AS TZAC,
    `Debtors > 6 months` AS `Debtors > 6 months`,
    Debtors AS Debtors,
    `ROL Amount` AS `ROL Amount`,
    `RUP Amount` AS `RUP Amount`,
    `RWP Amount` AS `RWP Amount`,
    `Days OS` AS `Days OS`,
    `Trading Partner` AS `Trading Partner`,
    Rating AS Rating,
    Resident AS Resident,
    `Debtor Category` AS `Debtor Category`,
    Deemed AS Deemed,
    Authorised AS Authorised,
    `Debtors Description` AS `Debtors Description`,
    Counterparty AS Counterparty,
    variableType AS variableType,
    Docs AS Docs,
    `Govn Law` AS `Govn Law`,
    `APRA Identifier` AS `APRA Identifier`,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    `Group Name` AS `Group Name`,
    `Group Domicile` AS `Group Domicile`,
    Country AS Country,
    `Share percent` AS `Share percent`,
    `Co-Ins` AS `Co-Ins`,
    `Occupation Type` AS `Occupation Type`,
    `Business Category` AS `Business Category`,
    Effective AS Effective,
    Valid AS Valid,
    Related AS Related,
    `APRA Grade` AS `APRA Grade`,
    variableTimestamp AS variableTimestamp,
    CAST(NULL AS STRING) AS `APRA Group`
  
  FROM Formula_473_0 AS in0

),

Summarize_475 AS (

  SELECT 
    SUM(`Debtors > 6 months`) AS `Debtors > 6 months`,
    SUM(Debtors) AS Debtors,
    SUM(`ROL Amount`) AS `ROL Amount`,
    SUM(`RUP Amount`) AS `RUP Amount`,
    SUM(`RWP Amount`) AS `RWP Amount`,
    SUM(`Days OS`) AS `Days OS`,
    `Govn Law` AS `Govn Law`,
    `Debtor Category` AS `Debtor Category`,
    Reinsurer AS Reinsurer,
    Rating AS Rating,
    Authorised AS Authorised,
    Company AS Company,
    `Group Name` AS `Group Name`,
    Deemed AS Deemed,
    Country AS Country,
    `Trading Partner` AS `Trading Partner`,
    `Business Category` AS `Business Category`,
    `Debtors Description` AS `Debtors Description`,
    `Account Type` AS `Account Type`,
    `Occupation Type` AS `Occupation Type`,
    `Counterparty Domicile` AS `Counterparty Domicile`,
    Counterparty AS Counterparty,
    `APRA Grade` AS `APRA Grade`,
    Effective AS Effective,
    `Group Domicile` AS `Group Domicile`,
    Docs AS Docs,
    Period AS Period,
    Valid AS Valid,
    Related AS Related,
    `Accident Year` AS `Accident Year`,
    `Line Number` AS `Line Number`,
    variableTimestamp AS variableTimestamp,
    `Product Code` AS `Product Code`,
    `APRA Identifier` AS `APRA Identifier`,
    TZAC AS TZAC,
    Branch AS Branch,
    `Co-Ins` AS `Co-Ins`,
    `RI Type IPS` AS `RI Type IPS`,
    `Share percent` AS `Share percent`,
    `Premium Class` AS `Premium Class`,
    `RI Type` AS `RI Type`,
    Resident AS Resident,
    variableType AS variableType
  
  FROM AlteryxSelect_472 AS in0
  
  GROUP BY 
    `Govn Law`, 
    `Debtor Category`, 
    Reinsurer, 
    Rating, 
    Authorised, 
    Company, 
    `Group Name`, 
    Deemed, 
    Country, 
    `Trading Partner`, 
    `Business Category`, 
    `Debtors Description`, 
    `Account Type`, 
    `Occupation Type`, 
    `Counterparty Domicile`, 
    Counterparty, 
    `APRA Grade`, 
    Effective, 
    `Group Domicile`, 
    Docs, 
    Period, 
    Valid, 
    Related, 
    `Accident Year`, 
    `Line Number`, 
    variableTimestamp, 
    `Product Code`, 
    `APRA Identifier`, 
    TZAC, 
    Branch, 
    `Co-Ins`, 
    `RI Type IPS`, 
    `Share percent`, 
    `Premium Class`, 
    `RI Type`, 
    Resident, 
    variableType

),

Formula_532_0 AS (

  SELECT 
    CAST((
      CONCAT(
        'APRA_Output_', 
        Period, 
        '_', 
        (DATE_FORMAT((TO_DATE(variableTimestamp, 'yyyy-MM-dd')), 'yyyy-MM-dd HH_mm_ss')), 
        '.csv')
    ) AS STRING) AS Filename,
    *
  
  FROM Summarize_475 AS in0

)

SELECT *

FROM Formula_532_0
