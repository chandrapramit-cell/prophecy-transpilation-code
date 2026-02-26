{{
  config({    
    "materialized": "table",
    "alias": "PLACEHOLDER_488",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_532_0 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_532_0')}}

),

AlteryxSelect_540 AS (

  SELECT 
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
    Filename AS Filename
  
  FROM Formula_532_0 AS in0

)

SELECT *

FROM AlteryxSelect_540
