{{
  config({    
    "materialized": "table",
    "alias": "table_3118_Input_macro_ip",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Savvas_Jul26_xl_2449 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'Savvas_Jul26_xl_2449'
    )
  }}

),

Formula_2451_0 AS (

  SELECT 
    (TO_DATE(`Term Start Date`)) AS StartDateAnnualization,
    (TO_DATE(`Term End Date`)) AS EndDateAnnualization,
    (TO_DATE(variableDate)) AS `Activated Date`,
    *
  
  FROM Savvas_Jul26_xl_2449 AS in0

),

AlteryxSelect_2452 AS (

  SELECT 
    `Internal ID` AS `Internal ID`,
    variableType AS variableType,
    `Document Number` AS `Document Number`,
    Name AS Name,
    StartDateAnnualization AS StartDateAnnualization,
    EndDateAnnualization AS EndDateAnnualization,
    Item AS Item,
    Amount AS Amount,
    `Activated Date` AS `Activated Date`,
    * EXCEPT (`variableDate`, 
    `Term Start Date`, 
    `Term End Date`, 
    `Internal ID`, 
    `variableType`, 
    `Document Number`, 
    `Name`, 
    `StartDateAnnualization`, 
    `EndDateAnnualization`, 
    `Item`, 
    `Amount`, 
    `Activated Date`)
  
  FROM Formula_2451_0 AS in0

),

Filter_2453 AS (

  SELECT * 
  
  FROM AlteryxSelect_2452 AS in0
  
  WHERE (NOT((`Internal ID` IS NULL) OR ((LENGTH(CAST(`Internal ID` AS string))) = 0)))

),

AlteryxSelect_2454 AS (

  SELECT 
    Name AS CustomerName,
    * EXCEPT (`Internal ID`, `variableType`, `Document Number`, `Item`, `Name`)
  
  FROM Filter_2453 AS in0

),

Formula_2455_to_Formula_2456_0 AS (

  SELECT 
    CAST('ReadyRosie' AS string) AS Product,
    CAST(CASE
      WHEN (
        (StartDateAnnualization <= to_date('2016-02-28'))
        AND (EndDateAnnualization >= to_date('2016-02-29'))
      )
        THEN CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT)
      WHEN (
        (StartDateAnnualization <= to_date('2020-02-28'))
        AND (EndDateAnnualization >= to_date('2020-02-29'))
      )
        THEN CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT)
      WHEN (
        (StartDateAnnualization <= to_date('2024-02-28'))
        AND (EndDateAnnualization >= to_date('2024-02-29'))
      )
        THEN CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT)
      WHEN (
        (StartDateAnnualization <= to_date('2028-02-28'))
        AND (EndDateAnnualization >= to_date('2028-02-29'))
      )
        THEN CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT)
      ELSE (CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT) + 1)
    END AS DOUBLE) AS TS_ContractDays,
    CAST(CASE
      WHEN (
        (StartDateAnnualization <= to_date('2016-02-28'))
        AND (EndDateAnnualization >= to_date('2016-02-29'))
      )
        THEN CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT)
      WHEN (
        (StartDateAnnualization <= to_date('2020-02-28'))
        AND (EndDateAnnualization >= to_date('2020-02-29'))
      )
        THEN CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT)
      WHEN (
        (StartDateAnnualization <= to_date('2024-02-28'))
        AND (EndDateAnnualization >= to_date('2024-02-29'))
      )
        THEN CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT)
      WHEN (
        (StartDateAnnualization <= to_date('2028-02-28'))
        AND (EndDateAnnualization >= to_date('2028-02-29'))
      )
        THEN CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT)
      ELSE (CAST(datediff(to_date(EndDateAnnualization), to_date(StartDateAnnualization)) AS INT) + 1)
    END AS DOUBLE) AS Engine_ContractDays,
    *
  
  FROM AlteryxSelect_2454 AS in0

),

Formula_2455_to_Formula_2456_1 AS (

  SELECT 
    CAST(((Amount / TS_ContractDays) * 365.25) AS DOUBLE) AS ACV,
    *
  
  FROM Formula_2455_to_Formula_2456_0 AS in0

),

Formula_2455_to_Formula_2456_2 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    CAST(0 AS DOUBLE) AS Quantity,
    *
  
  FROM Formula_2455_to_Formula_2456_1 AS in0

),

AlteryxSelect_2465 AS (

  SELECT 
    StartDateAnnualization AS ContractStartDate,
    EndDateAnnualization AS ContractEndDate,
    * EXCEPT (`StartDateAnnualization`, `EndDateAnnualization`)
  
  FROM Formula_2455_to_Formula_2456_2 AS in0

),

Formula_2479_to_Formula_2469_0 AS (

  SELECT 
    CAST('Other' AS string) AS Sector,
    CAST('Other' AS string) AS variableType,
    CAST('Other' AS string) AS `Territory Name`,
    CAST('Other' AS string) AS State,
    CAST('Other' AS string) AS `Account Owner`,
    CAST('Other' AS string) AS `Partner Success Owner`,
    CAST('ReadyRosie' AS string) AS Product,
    CAST('Other' AS string) AS `Account Size`,
    CAST(0 AS DOUBLE) AS Quantity,
    CAST(NULL AS string) AS Stage,
    (TO_DATE(NULL, 'yyyy-MM-dd')) AS `Created Date`,
    CAST(NULL AS string) AS `Order: Sales Order Number`,
    CAST(NULL AS string) AS `Product Code`,
    (TO_DATE(NULL, 'yyyy-MM-dd')) AS `Actual Closed Date`,
    CAST(NULL AS string) AS Origin,
    (LAST_DAY(CAST(`Activated Date` AS DATE))) AS `ARR Period`,
    * EXCEPT (`quantity`, `product`)
  
  FROM AlteryxSelect_2465 AS in0

),

Formula_2479_to_Formula_2469_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((Quantity IS NULL) AS BOOLEAN)
          THEN 0
        WHEN ((Quantity IS NULL) OR ((LENGTH(CAST(Quantity AS string))) = 0))
          THEN 0
        ELSE Quantity
      END
    ) AS DOUBLE) AS Quantity,
    * EXCEPT (`quantity`)
  
  FROM Formula_2479_to_Formula_2469_0 AS in0

),

Macro_3118_Input AS (

  SELECT 
    CustomerName AS CustomerName,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    Product AS Product,
    `Product Code` AS `Product Code`,
    ContractStartDate AS ContractStartDate,
    ContractEndDate AS ContractEndDate,
    TCV AS TCV,
    Quantity AS Quantity,
    Origin AS Origin,
    `Actual Closed Date` AS `Actual Closed Date`,
    `Created Date` AS `Created Date`,
    Stage AS Stage,
    Sector AS Sector,
    variableType AS variableType,
    `Territory Name` AS `Territory Name`,
    State AS State,
    `Account Owner` AS `Account Owner`,
    `Partner Success Owner` AS `Partner Success Owner`,
    `ARR Period` AS `ARR Period`,
    * EXCEPT (`CustomerName`, 
    `Order: Sales Order Number`, 
    `Product`, 
    `Product Code`, 
    `ContractStartDate`, 
    `ContractEndDate`, 
    `TCV`, 
    `Quantity`, 
    `Origin`, 
    `Actual Closed Date`, 
    `Created Date`, 
    `Stage`, 
    `Sector`, 
    `variableType`, 
    `Territory Name`, 
    `State`, 
    `Account Owner`, 
    `Partner Success Owner`, 
    `ARR Period`)
  
  FROM Formula_2479_to_Formula_2469_1 AS in0

)

SELECT *

FROM Macro_3118_Input
