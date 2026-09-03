{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_3233 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3233')}}

),

DateTime_3239_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`QB Date`, 'yyyy-MM-dd')), 'yyyy-MM-dd')) AS `QB Date Out`,
    *
  
  FROM AlteryxSelect_3233 AS in0

),

DateTime_3234_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`QB Date`, 'yyyy-MM-dd')), 'yyyy-MM-dd')) AS DateTime_Out,
    *
  
  FROM DateTime_3239_0 AS in0

),

AlteryxSelect_3235 AS (

  SELECT 
    Mas90 AS CustomerName,
    CAST(Revenue AS DOUBLE) AS Revenue,
    `QB Date Out` AS `Activated Date`,
    DateTime_Out AS variableDate,
    * EXCEPT (`QB Date`, `variableDate`, `Customer Name2`, `Revenue`, `Mas90`, `QB Date Out`, `DateTime_Out`)
  
  FROM DateTime_3234_0 AS in0

),

Formula_3236_to_Formula_3237_0 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', variableDate)))) AS ContractStartDate,
    *
  
  FROM AlteryxSelect_3235 AS in0

),

AlteryxSelect_3342 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3342')}}

),

AlteryxSelect_2510 AS (

  SELECT 
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    Sector AS Sector,
    `Territory Name` AS `Territory Name`,
    variableType AS variableType,
    `Partner Success Owner` AS `Partner Success Owner`,
    `Account Owner` AS `Account Owner`
  
  FROM AlteryxSelect_3342 AS in0

),

Filter_2509 AS (

  SELECT * 
  
  FROM AlteryxSelect_2510 AS in0
  
  WHERE (NOT(`Mas90 Customer Number` IS NULL))

),

Summarize_2508 AS (

  SELECT DISTINCT `Mas90 Customer Number` AS `Mas90 Customer Number`
  
  FROM Filter_2509 AS in0

),

Unique_2507 AS (

  SELECT * 
  
  FROM Summarize_2508 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `Mas90 Customer Number` ORDER BY `Mas90 Customer Number`) = 1

),

AlteryxSelect_2490 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2490')}}

),

DateTime_2496_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`QB Date`, 'yyyy-MM-dd')), 'yyyy-MM-dd')) AS `QB Date Out`,
    *
  
  FROM AlteryxSelect_2490 AS in0

),

DateTime_2491_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`QB Date`, 'yyyy-MM-dd')), 'yyyy-MM-dd')) AS DateTime_Out,
    *
  
  FROM DateTime_2496_0 AS in0

),

AlteryxSelect_2492 AS (

  SELECT 
    Mas90 AS CustomerName,
    CAST(Revenue AS DOUBLE) AS Revenue,
    `QB Date Out` AS `Activated Date`,
    DateTime_Out AS variableDate,
    * EXCEPT (`QB Date`, `variableDate`, `Customer Name2`, `Revenue`, `Mas90`, `QB Date Out`, `DateTime_Out`)
  
  FROM DateTime_2491_0 AS in0

),

Formula_2493_to_Formula_2494_0 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', variableDate)))) AS ContractStartDate,
    *
  
  FROM AlteryxSelect_2492 AS in0

),

Formula_2493_to_Formula_2494_1 AS (

  SELECT 
    (TO_DATE((DATE_ADD((ADD_MONTHS(ContractStartDate, 1)), CAST(-1 AS INTEGER))))) AS ContractEndDate,
    CAST('Other' AS string) AS Sector,
    CAST('Other' AS string) AS variableType,
    CAST('Other' AS string) AS `Territory Name`,
    CAST('Other' AS string) AS State,
    CAST('Other' AS string) AS `Account Owner`,
    CAST('Other' AS string) AS `Partner Success Owner`,
    CAST('Tadpoles' AS string) AS Product,
    CAST('Other' AS string) AS `Account Size`,
    CAST((Revenue * 12) AS DOUBLE) AS ARR,
    CAST(0 AS DOUBLE) AS Quantity,
    (TO_DATE((DATE_TRUNC('month', variableDate)), 'yyyy-MM-dd')) AS variableDate,
    * EXCEPT (`variabledate`)
  
  FROM Formula_2493_to_Formula_2494_0 AS in0

),

Formula_2493_to_Formula_2494_2 AS (

  SELECT 
    (TO_DATE((LAST_DAY(CAST(variableDate AS DATE))))) AS StaticHistoryMonth,
    *
  
  FROM Formula_2493_to_Formula_2494_1 AS in0

),

Formula_2493_to_Formula_2494_3 AS (

  SELECT 
    (
      TO_DATE(
        (
          CONCAT(
            (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST(EXTRACT(YEAR FROM StaticHistoryMonth) AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            ), 
            '-12-31')
        ))
    ) AS StaticHistoryYearEnd,
    CAST(70 AS DOUBLE) AS MaxIteration,
    *
  
  FROM Formula_2493_to_Formula_2494_2 AS in0

),

AlteryxSelect_2495 AS (

  SELECT 
    Revenue AS MRR,
    variableDate AS RevMonth,
    * EXCEPT (`Activated Date`, `ContractStartDate`, `ContractEndDate`, `Revenue`, `variableDate`)
  
  FROM Formula_2493_to_Formula_2494_3 AS in0

),

Join_2506_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Mas90 Customer Number`)
  
  FROM AlteryxSelect_2495 AS in0
  INNER JOIN Unique_2507 AS in1
     ON (in0.CustomerName = in1.`Mas90 Customer Number`)

),

Summarize_2505 AS (

  SELECT DISTINCT CustomerName AS CustomerName
  
  FROM Join_2506_inner AS in0

),

RecordID_2504 AS (

  {{
    prophecy_basics.RecordID(
      ['Summarize_2505'], 
      'incremental_id', 
      'RecordID', 
      'string', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Formula_2503_0 AS (

  SELECT 
    CAST((
      CONCAT(
        'Customer', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(RecordID AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS `Anonymized Customer Name`,
    *
  
  FROM RecordID_2504 AS in0

),

Join_2502_inner AS (

  SELECT 
    in1.`Anonymized Customer Name` AS CustomerName,
    in0.Product AS Product,
    in0.`Account Size` AS `Account Size`,
    in0.Sector AS Sector,
    in0.variableType AS variableType,
    in0.`Territory Name` AS `Territory Name`,
    in0.State AS State,
    in0.`Account Owner` AS `Account Owner`,
    in0.`Partner Success Owner` AS `Partner Success Owner`,
    in0.RevMonth AS RevMonth,
    in0.Quantity AS Quantity,
    in0.ARR AS ARR,
    in0.StaticHistoryMonth AS StaticHistoryMonth,
    in0.MaxIteration AS MaxIteration,
    in0.StaticHistoryYearEnd AS StaticHistoryYearEnd,
    in0.MRR AS MRR,
    in1.RecordID AS RecordID,
    in0.* EXCEPT (`CustomerName`, 
    `Product`, 
    `Account Size`, 
    `Sector`, 
    `variableType`, 
    `Territory Name`, 
    `State`, 
    `Account Owner`, 
    `Partner Success Owner`, 
    `RevMonth`, 
    `Quantity`, 
    `ARR`, 
    `StaticHistoryMonth`, 
    `MaxIteration`, 
    `StaticHistoryYearEnd`, 
    `MRR`),
    in1.* EXCEPT (`Anonymized Customer Name`, `RecordID`, `CustomerName`)
  
  FROM Join_2506_inner AS in0
  INNER JOIN Formula_2503_0 AS in1
     ON (in0.CustomerName = in1.CustomerName)

),

Formula_3236_to_Formula_3237_1 AS (

  SELECT 
    (TO_DATE((DATE_ADD((ADD_MONTHS(ContractStartDate, 1)), CAST(-1 AS INTEGER))))) AS ContractEndDate,
    CAST('Other' AS string) AS Sector,
    CAST('Other' AS string) AS variableType,
    CAST('Other' AS string) AS `Territory Name`,
    CAST('Other' AS string) AS State,
    CAST('Other' AS string) AS `Account Owner`,
    CAST('Other' AS string) AS `Partner Success Owner`,
    CAST('Tadpoles' AS string) AS Product,
    CAST('Other' AS string) AS `Account Size`,
    CAST((Revenue * 12) AS DOUBLE) AS ARR,
    CAST(0 AS DOUBLE) AS Quantity,
    (TO_DATE((DATE_TRUNC('month', variableDate)), 'yyyy-MM-dd')) AS variableDate,
    * EXCEPT (`variabledate`)
  
  FROM Formula_3236_to_Formula_3237_0 AS in0

),

Formula_3236_to_Formula_3237_2 AS (

  SELECT 
    (TO_DATE((LAST_DAY(CAST(variableDate AS DATE))))) AS StaticHistoryMonth,
    *
  
  FROM Formula_3236_to_Formula_3237_1 AS in0

),

Formula_3236_to_Formula_3237_3 AS (

  SELECT 
    (
      TO_DATE(
        (
          CONCAT(
            (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST(EXTRACT(YEAR FROM StaticHistoryMonth) AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            ), 
            '-12-31')
        ))
    ) AS StaticHistoryYearEnd,
    CAST(70 AS DOUBLE) AS MaxIteration,
    *
  
  FROM Formula_3236_to_Formula_3237_2 AS in0

),

AlteryxSelect_3238 AS (

  SELECT 
    Revenue AS MRR,
    variableDate AS RevMonth,
    * EXCEPT (`Activated Date`, `ContractStartDate`, `ContractEndDate`, `Revenue`, `variableDate`)
  
  FROM Formula_3236_to_Formula_3237_3 AS in0

),

Filter_3248 AS (

  SELECT * 
  
  FROM AlteryxSelect_2510 AS in0
  
  WHERE (NOT(`Mas90 Customer Number` IS NULL))

),

Summarize_3247 AS (

  SELECT DISTINCT `Mas90 Customer Number` AS `Mas90 Customer Number`
  
  FROM Filter_3248 AS in0

),

Unique_3246 AS (

  SELECT * 
  
  FROM Summarize_3247 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `Mas90 Customer Number` ORDER BY `Mas90 Customer Number`) = 1

),

Join_3245_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Mas90 Customer Number`)
  
  FROM AlteryxSelect_3238 AS in0
  INNER JOIN Unique_3246 AS in1
     ON (in0.CustomerName = in1.`Mas90 Customer Number`)

),

Summarize_3244 AS (

  SELECT DISTINCT CustomerName AS CustomerName
  
  FROM Join_3245_inner AS in0

),

RecordID_3243 AS (

  {{
    prophecy_basics.RecordID(
      ['Summarize_3244'], 
      'incremental_id', 
      'RecordID', 
      'string', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Formula_3242_0 AS (

  SELECT 
    CAST((
      CONCAT(
        'Customer', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(RecordID AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
    ) AS string) AS `Anonymized Customer Name`,
    *
  
  FROM RecordID_3243 AS in0

),

Join_3241_inner AS (

  SELECT 
    in1.`Anonymized Customer Name` AS CustomerName,
    in0.Product AS Product,
    in0.`Account Size` AS `Account Size`,
    in0.Sector AS Sector,
    in0.variableType AS variableType,
    in0.`Territory Name` AS `Territory Name`,
    in0.State AS State,
    in0.`Account Owner` AS `Account Owner`,
    in0.`Partner Success Owner` AS `Partner Success Owner`,
    in0.RevMonth AS RevMonth,
    in0.Quantity AS Quantity,
    in0.ARR AS ARR,
    in0.StaticHistoryMonth AS StaticHistoryMonth,
    in0.MaxIteration AS MaxIteration,
    in0.StaticHistoryYearEnd AS StaticHistoryYearEnd,
    in0.MRR AS MRR,
    in1.RecordID AS RecordID,
    in0.* EXCEPT (`CustomerName`, 
    `Product`, 
    `Account Size`, 
    `Sector`, 
    `variableType`, 
    `Territory Name`, 
    `State`, 
    `Account Owner`, 
    `Partner Success Owner`, 
    `RevMonth`, 
    `Quantity`, 
    `ARR`, 
    `StaticHistoryMonth`, 
    `MaxIteration`, 
    `StaticHistoryYearEnd`, 
    `MRR`),
    in1.* EXCEPT (`Anonymized Customer Name`, `RecordID`, `CustomerName`)
  
  FROM Join_3245_inner AS in0
  INNER JOIN Formula_3242_0 AS in1
     ON (in0.CustomerName = in1.CustomerName)

),

Join_3245_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_3238 AS in0
  ANTI JOIN Unique_3246 AS in1
     ON (in0.CustomerName = in1.`Mas90 Customer Number`)

),

Union_3240 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_3241_inner', 'Join_3245_left'], 
      [
        '[{"name": "CustomerName", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Account Size", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "MRR", "dataType": "Double"}, {"name": "RecordID", "dataType": "String"}]', 
        '[{"name": "MRR", "dataType": "Double"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Sector", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Account Size", "dataType": "String"}, {"name": "ARR", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "CustomerName", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_2506_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_2495 AS in0
  ANTI JOIN Unique_2507 AS in1
     ON (in0.CustomerName = in1.`Mas90 Customer Number`)

),

Summarize_3250 AS (

  SELECT 
    SUM(Quantity) AS Quantity,
    SUM(ARR) AS ARR,
    SUM(MRR) AS MRR,
    StaticHistoryYearEnd AS StaticHistoryYearEnd,
    Sector AS Sector,
    `Territory Name` AS `Territory Name`,
    CustomerName AS CustomerName,
    RevMonth AS RevMonth,
    MaxIteration AS MaxIteration,
    `Account Size` AS `Account Size`,
    StaticHistoryMonth AS StaticHistoryMonth,
    `Account Owner` AS `Account Owner`,
    Product AS Product,
    State AS State,
    `Partner Success Owner` AS `Partner Success Owner`,
    variableType AS variableType
  
  FROM Union_3240 AS in0
  
  GROUP BY 
    StaticHistoryYearEnd, 
    Sector, 
    `Territory Name`, 
    CustomerName, 
    RevMonth, 
    MaxIteration, 
    `Account Size`, 
    StaticHistoryMonth, 
    `Account Owner`, 
    Product, 
    State, 
    `Partner Success Owner`, 
    variableType

),

Filter_3257 AS (

  SELECT * 
  
  FROM Summarize_3250 AS in0
  
  WHERE (RevMonth < to_date('2023-01-01'))

),

Union_2501 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_2502_inner', 'Join_2506_left'], 
      [
        '[{"name": "CustomerName", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Account Size", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "MRR", "dataType": "Double"}, {"name": "RecordID", "dataType": "String"}]', 
        '[{"name": "MRR", "dataType": "Double"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Sector", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Account Size", "dataType": "String"}, {"name": "ARR", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "CustomerName", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_2512 AS (

  SELECT 
    SUM(Quantity) AS Quantity,
    SUM(ARR) AS ARR,
    SUM(MRR) AS MRR,
    StaticHistoryYearEnd AS StaticHistoryYearEnd,
    Sector AS Sector,
    `Territory Name` AS `Territory Name`,
    CustomerName AS CustomerName,
    RevMonth AS RevMonth,
    MaxIteration AS MaxIteration,
    `Account Size` AS `Account Size`,
    StaticHistoryMonth AS StaticHistoryMonth,
    `Account Owner` AS `Account Owner`,
    Product AS Product,
    State AS State,
    `Partner Success Owner` AS `Partner Success Owner`,
    variableType AS variableType
  
  FROM Union_2501 AS in0
  
  GROUP BY 
    StaticHistoryYearEnd, 
    Sector, 
    `Territory Name`, 
    CustomerName, 
    RevMonth, 
    MaxIteration, 
    `Account Size`, 
    StaticHistoryMonth, 
    `Account Owner`, 
    Product, 
    State, 
    `Partner Success Owner`, 
    variableType

),

table_3118_Exit_macro_op AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_3118_Exit_macro_op') }}

),

Formula_2477_0 AS (

  SELECT 
    (LAST_DAY(CAST(RevMonth AS DATE))) AS RevMonth,
    * EXCEPT (`revmonth`)
  
  FROM table_3118_Exit_macro_op AS in0

),

Filter_2473 AS (

  SELECT * 
  
  FROM Formula_2477_0 AS in0
  
  WHERE (RevMonth = StaticHistoryMonth)

),

Formula_2474_to_Formula_2480_0 AS (

  SELECT 
    (TO_DATE(StaticHistoryMonth, 'yyyy-MM-dd')) AS RevMonth,
    * EXCEPT (`revmonth`)
  
  FROM Filter_2473 AS in0

),

Formula_2474_to_Formula_2480_1 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', RevMonth)), 'yyyy-MM-dd')) AS RevMonth,
    CAST('Other' AS string) AS Sector,
    CAST('Other' AS string) AS variableType,
    CAST('Other' AS string) AS `Territory Name`,
    CAST('Other' AS string) AS State,
    CAST('Other' AS string) AS `Account Owner`,
    CAST('Other' AS string) AS `Partner Success Owner`,
    CAST('ReadyRosie' AS string) AS Product,
    CAST('Other' AS string) AS `Account Size`,
    CAST(0 AS DOUBLE) AS Quantity,
    * EXCEPT (`quantity`, 
    `sector`, 
    `territory name`, 
    `revmonth`, 
    `account size`, 
    `account owner`, 
    `product`, 
    `state`, 
    `partner success owner`, 
    `variabletype`)
  
  FROM Formula_2474_to_Formula_2480_0 AS in0

),

Formula_2474_to_Formula_2480_2 AS (

  SELECT 
    (TO_DATE((LAST_DAY(CAST(RevMonth AS DATE))))) AS StaticHistoryMonth,
    * EXCEPT (`statichistorymonth`)
  
  FROM Formula_2474_to_Formula_2480_1 AS in0

),

Formula_2474_to_Formula_2480_3 AS (

  SELECT 
    (
      TO_DATE(
        (
          CONCAT(
            (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST(EXTRACT(YEAR FROM StaticHistoryMonth) AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            ), 
            '-12-31')
        ))
    ) AS StaticHistoryYearEnd,
    CAST(70 AS DOUBLE) AS MaxIteration,
    * EXCEPT (`statichistoryyearend`, `maxiteration`)
  
  FROM Formula_2474_to_Formula_2480_2 AS in0

),

AlteryxSelect_2481 AS (

  SELECT * EXCEPT (`YetToRenewARR`)
  
  FROM Formula_2474_to_Formula_2480_3 AS in0

),

Union_2516 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_2512', 'Filter_3257', 'AlteryxSelect_2481'], 
      [
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "MRR", "dataType": "Double"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Sector", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "MRR", "dataType": "Double"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Sector", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Sector", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Account Size", "dataType": "String"}, {"name": "Quantity", "dataType": "Double"}, {"name": "CustomerName", "dataType": "String"}, {"name": "ARR", "dataType": "Double"}, {"name": "MRR", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_3344_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (ARR < 0)
          THEN 0
        ELSE ARR
      END
    ) AS DOUBLE) AS ARR,
    * EXCEPT (`arr`)
  
  FROM Union_2516 AS in0

),

Filter_2313 AS (

  SELECT * 
  
  FROM Formula_3344_0 AS in0
  
  WHERE (RevMonth >= to_date(date_trunc('month', {{ var('User__Current_Period') }})))

)

SELECT *

FROM Filter_2313
