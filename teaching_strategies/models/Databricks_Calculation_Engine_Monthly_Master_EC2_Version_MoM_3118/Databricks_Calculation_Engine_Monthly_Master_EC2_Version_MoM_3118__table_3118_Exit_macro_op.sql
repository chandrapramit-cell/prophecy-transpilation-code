{{
  config({    
    "materialized": "incremental",
    "alias": "table_3118_Exit_macro_op",
    "database": "sony",
    "incremental_strategy": "append",
    "schema": "orch_test"
  })
}}

WITH RecordID_1270_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__RecordID_1270_3118')}}

),

Formula_845_3118_1 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Formula_845_3118_1')}}

),

Filter_1273_3118 AS (

  SELECT * 
  
  FROM Formula_845_3118_1 AS in0
  
  WHERE CAST(`Open Renewal Flag` AS BOOLEAN)

),

Join_1272_3118_inner AS (

  SELECT 
    in0.* EXCEPT (`RecordID`, 
    `CustomerName`, 
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
    `ARR Period`, 
    `StaticHistoryMonth`, 
    `StaticHistoryYearEnd`, 
    `Open Renewal Flag`, 
    `Amount`, 
    `ACV`, 
    `Activated Date`, 
    `TS_ContractDays`, 
    `Account Size`, 
    `Engine_ContractDays`),
    in1.*
  
  FROM Filter_1273_3118 AS in0
  INNER JOIN RecordID_1270_3118 AS in1
     ON (in0.RecordID = in1.RecordID)

),

AlteryxSelect_852_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__AlteryxSelect_852_3118')}}

),

Filter_844_3118_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_852_3118 AS in0
  
  WHERE (
          (
            NOT(
              `ARR Period` <= StaticHistoryYearEnd)
          )
          OR ((`ARR Period` <= StaticHistoryYearEnd) IS NULL)
        )

),

Union_1269_3118 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_844_3118_reject', 'Join_1272_3118_inner'], 
      [
        '[{"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "Amount", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Activated Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]', 
        '[{"name": "RecordID", "dataType": "Integer"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "Amount", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Activated Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_1262_3118 AS (

  SELECT * 
  
  FROM Union_1269_3118 AS in0
  
  WHERE (`Created Date` <= StaticHistoryMonth)

),

Formula_1263_3118_0 AS (

  SELECT 
    CAST(CASE
      WHEN ((ContractStartDate <= to_date('2016-02-28')) AND (ContractEndDate >= to_date('2016-02-29')))
        THEN CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT)
      WHEN ((ContractStartDate <= to_date('2020-02-28')) AND (ContractEndDate >= to_date('2020-02-29')))
        THEN CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT)
      WHEN ((ContractStartDate <= to_date('2024-02-28')) AND (ContractEndDate >= to_date('2024-02-29')))
        THEN CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT)
      WHEN ((ContractStartDate <= to_date('2028-02-28')) AND (ContractEndDate >= to_date('2028-02-29')))
        THEN CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT)
      ELSE (CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT) + 1)
    END AS DOUBLE) AS ContractTermDays,
    *
  
  FROM Filter_1262_3118 AS in0

),

Summarize_848_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Summarize_848_3118')}}

),

Filter_863_3118_to_Filter_870_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_863_3118_to_Filter_870_3118')}}

),

Summarize_865_3118 AS (

  SELECT 
    SUM(ARR) AS ARR,
    SUM(Quantity) AS Quantity,
    Sector AS Sector,
    `Territory Name` AS `Territory Name`,
    CustomerName AS CustomerName,
    `Account Owner` AS `Account Owner`,
    Product AS Product,
    State AS State,
    `Partner Success Owner` AS `Partner Success Owner`,
    variableType AS variableType,
    ARRMonth AS ARRMonth
  
  FROM Filter_863_3118_to_Filter_870_3118 AS in0
  
  GROUP BY 
    Sector, 
    `Territory Name`, 
    CustomerName, 
    `Account Owner`, 
    Product, 
    State, 
    `Partner Success Owner`, 
    variableType, 
    ARRMonth

),

AppendFields_871_3118 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_848_3118 AS in0
  INNER JOIN Summarize_865_3118 AS in1
     ON TRUE

),

Formula_1263_3118_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (ContractTermDays <= 0)
          THEN 0
        ELSE (
          (
            CASE
              WHEN (
                ((((TCV / ContractTermDays) * 365.25) / 0.01) < 0)
                AND (((((TCV / ContractTermDays) * 365.25) / 0.01) - FLOOR((((TCV / ContractTermDays) * 365.25) / 0.01))) = 0.5)
              )
                THEN CEIL((((TCV / ContractTermDays) * 365.25) / 0.01))
              ELSE ROUND((((TCV / ContractTermDays) * 365.25) / 0.01))
            END
          )
          * 0.01
        )
      END
    ) AS DOUBLE) AS YetToRenewARR,
    CAST((
      (
        CASE
          WHEN (
            (((ContractTermDays / 30.4375) / 0.1) < 0)
            AND ((((ContractTermDays / 30.4375) / 0.1) - FLOOR(((ContractTermDays / 30.4375) / 0.1))) = 0.5)
          )
            THEN CEIL(((ContractTermDays / 30.4375) / 0.1))
          ELSE ROUND(((ContractTermDays / 30.4375) / 0.1))
        END
      )
      * 0.1
    ) AS DOUBLE) AS ContractTermMonths,
    (TO_DATE((DATE_TRUNC('month', `Created Date`)), 'yyyy-MM-dd')) AS YetToRenewStart,
    *
  
  FROM Formula_1263_3118_0 AS in0

),

Formula_1263_3118_2 AS (

  SELECT 
    (
      TO_DATE(
        (
          CASE
            WHEN CAST(((DATE_ADD((DATE_TRUNC('month', `Actual Closed Date`)), CAST(-1 AS INTEGER))) IS NULL) AS BOOLEAN)
              THEN (DATE_ADD((ADD_MONTHS(YetToRenewStart, ContractTermMonths)), CAST(-1 AS INTEGER)))
            ELSE (DATE_ADD((DATE_TRUNC('month', `Actual Closed Date`)), CAST(-1 AS INTEGER)))
          END
        ), 
        'yyyy-MM-dd')
    ) AS YetToRenewEnd,
    *
  
  FROM Formula_1263_3118_1 AS in0

),

GenerateRows_1264_3118 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Formula_1263_3118_2'], 
      '[{"name": "YetToRenewEnd", "dataType": "Date"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "ContractTermMonths", "dataType": "Double"}, {"name": "YetToRenewStart", "dataType": "Date"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "Amount", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Activated Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}, {"name": "RecordID", "dataType": "Integer"}]', 
      'last_day(payload.YetToRenewStart)', 
      '((YetToRenewMonth <= payload.YetToRenewEnd) AND (YetToRenewMonth <= concat(regexp_replace(regexp_replace(format_number(CAST(year(current_timestamp()) AS DOUBLE), 0), ",", "__THS__"), "__THS__", ""), "-12-31")))', 
      'last_day(add_months(YetToRenewMonth, 1))', 
      'YetToRenewMonth', 
      '100', 
      'recursive'
    )
  }}

),

Summarize_1266_3118 AS (

  SELECT 
    SUM(YetToRenewARR) AS YetToRenewARR,
    Sector AS Sector,
    `Territory Name` AS `Territory Name`,
    CustomerName AS CustomerName,
    YetToRenewMonth AS YetToRenewMonth,
    `Account Owner` AS `Account Owner`,
    Product AS Product,
    State AS State,
    `Partner Success Owner` AS `Partner Success Owner`,
    variableType AS variableType
  
  FROM GenerateRows_1264_3118 AS in0
  
  GROUP BY 
    Sector, 
    `Territory Name`, 
    CustomerName, 
    YetToRenewMonth, 
    `Account Owner`, 
    Product, 
    State, 
    `Partner Success Owner`, 
    variableType

),

Join_1267_3118_left_UnionLeftOuter AS (

  SELECT 
    in0.CustomerName AS CustomerName,
    in0.Product AS Product,
    in0.ARRMonth AS ARRMonth,
    in0.ARR AS ARR,
    in1.YetToRenewARR AS YetToRenewARR,
    in0.Sector AS Sector,
    in0.`Territory Name` AS `Territory Name`,
    in0.variableType AS variableType,
    in0.State AS State,
    in0.`Partner Success Owner` AS `Partner Success Owner`,
    in0.`Account Owner` AS `Account Owner`,
    in0.Quantity AS Quantity,
    in0.StaticHistoryMonth AS StaticHistoryMonth,
    in0.MaxIteration AS MaxIteration,
    in0.StaticHistoryYearEnd AS StaticHistoryYearEnd,
    in0.* EXCEPT (`CustomerName`, 
    `Product`, 
    `ARRMonth`, 
    `ARR`, 
    `Sector`, 
    `Territory Name`, 
    `variableType`, 
    `State`, 
    `Partner Success Owner`, 
    `Account Owner`, 
    `Quantity`, 
    `StaticHistoryMonth`, 
    `MaxIteration`, 
    `StaticHistoryYearEnd`),
    in1.* EXCEPT (`YetToRenewARR`, 
    `CustomerName`, 
    `YetToRenewMonth`, 
    `Product`, 
    `Sector`, 
    `Territory Name`, 
    `variableType`, 
    `State`, 
    `Partner Success Owner`, 
    `Account Owner`)
  
  FROM AppendFields_871_3118 AS in0
  LEFT JOIN Summarize_1266_3118 AS in1
     ON (
      (
        (
          (
            (
              (
                (
                  ((in0.CustomerName = in1.CustomerName) AND (in0.Product = in1.Product))
                  AND (in0.ARRMonth = in1.YetToRenewMonth)
                )
                AND (in0.Sector = in1.Sector)
              )
              AND (in0.`Territory Name` = in1.`Territory Name`)
            )
            AND (in0.variableType = in1.variableType)
          )
          AND (in0.State = in1.State)
        )
        AND (in0.`Partner Success Owner` = in1.`Partner Success Owner`)
      )
      AND (in0.`Account Owner` = in1.`Account Owner`)
    )

),

Filter_869_3118 AS (

  SELECT * 
  
  FROM Join_1267_3118_left_UnionLeftOuter AS in0
  
  WHERE (ARRMonth <= StaticHistoryYearEnd)

),

RecordID_430_3118 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_869_3118'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Cleanse_493_3118 AS (

  {{
    prophecy_basics.DataCleansing(
      ['RecordID_430_3118'], 
      [
        { "name": "RecordID", "dataType": "Integer" }, 
        { "name": "CustomerName", "dataType": "String" }, 
        { "name": "Product", "dataType": "String" }, 
        { "name": "ARRMonth", "dataType": "Date" }, 
        { "name": "ARR", "dataType": "Double" }, 
        { "name": "YetToRenewARR", "dataType": "Double" }, 
        { "name": "Sector", "dataType": "String" }, 
        { "name": "Territory Name", "dataType": "String" }, 
        { "name": "variableType", "dataType": "String" }, 
        { "name": "State", "dataType": "String" }, 
        { "name": "Partner Success Owner", "dataType": "String" }, 
        { "name": "Account Owner", "dataType": "String" }, 
        { "name": "Quantity", "dataType": "Double" }, 
        { "name": "StaticHistoryMonth", "dataType": "Date" }, 
        { "name": "MaxIteration", "dataType": "Double" }, 
        { "name": "StaticHistoryYearEnd", "dataType": "Date" }
      ], 
      'keepOriginal', 
      ['Quantity'], 
      true, 
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

Filter_490_3118 AS (

  SELECT * 
  
  FROM Cleanse_493_3118 AS in0
  
  WHERE (UPPER(Product) = UPPER('CC Cloud'))

),

Summarize_433_3118 AS (

  SELECT 
    SUM(Quantity) AS Quantity,
    CustomerName AS CustomerName,
    ARRMonth AS ARRMonth
  
  FROM Filter_490_3118 AS in0
  
  GROUP BY 
    CustomerName, ARRMonth

),

Formula_434_3118_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Quantity = 0)
          THEN '0 classrooms'
        WHEN ((Quantity >= 1) AND (Quantity <= 5))
          THEN '1-5 classrooms'
        WHEN ((Quantity >= 6) AND (Quantity <= 25))
          THEN '6-25 classrooms'
        WHEN ((Quantity >= 26) AND (Quantity <= 70))
          THEN '26-70 classrooms'
        ELSE '71+ classrooms'
      END
    ) AS string) AS `Account Size`,
    *
  
  FROM Summarize_433_3118 AS in0

),

Join_435_3118_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.CustomerName = in1.CustomerName) AND (in0.ARRMonth = in1.ARRMonth))
          THEN NULL
        ELSE '0 classrooms'
      END
    ) AS `Account Size`,
    in0.* EXCEPT (`Quantity`, `CustomerName`, `ARRMonth`),
    in1.* EXCEPT (`Account Size`)
  
  FROM RecordID_430_3118 AS in0
  LEFT JOIN Formula_434_3118_0 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.ARRMonth = in1.ARRMonth))

),

Formula_438_3118_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((Quantity IS NULL) AS BOOLEAN)
          THEN 0
        WHEN ((Quantity IS NULL) OR ((LENGTH(CAST(Quantity AS string))) = 0))
          THEN 0
        ELSE Quantity
      END
    ) AS INTEGER) AS Quantity,
    CAST((
      CASE
        WHEN CAST((ARR IS NULL) AS BOOLEAN)
          THEN 0
        WHEN ((ARR IS NULL) OR ((LENGTH(CAST(ARR AS string))) = 0))
          THEN 0
        ELSE ARR
      END
    ) AS DOUBLE) AS ARR,
    CAST((
      CASE
        WHEN CAST((YetToRenewARR IS NULL) AS BOOLEAN)
          THEN 0
        WHEN ((YetToRenewARR IS NULL) OR ((LENGTH(CAST(YetToRenewARR AS string))) = 0))
          THEN 0
        ELSE YetToRenewARR
      END
    ) AS DOUBLE) AS YetToRenewARR,
    * EXCEPT (`quantity`, `yettorenewarr`, `arr`)
  
  FROM Join_435_3118_inner_UnionLeftOuter AS in0

),

Summarize_436_3118 AS (

  SELECT 
    SUM(Quantity) AS Quantity,
    SUM(ARR) AS ARR,
    StaticHistoryYearEnd AS StaticHistoryYearEnd,
    YetToRenewARR AS YetToRenewARR,
    Sector AS Sector,
    `Territory Name` AS `Territory Name`,
    CustomerName AS CustomerName,
    MaxIteration AS MaxIteration,
    `Account Size` AS `Account Size`,
    StaticHistoryMonth AS StaticHistoryMonth,
    `Account Owner` AS `Account Owner`,
    Product AS Product,
    State AS State,
    `Partner Success Owner` AS `Partner Success Owner`,
    variableType AS variableType,
    ARRMonth AS ARRMonth
  
  FROM Formula_438_3118_0 AS in0
  
  GROUP BY 
    StaticHistoryYearEnd, 
    YetToRenewARR, 
    Sector, 
    `Territory Name`, 
    CustomerName, 
    MaxIteration, 
    `Account Size`, 
    StaticHistoryMonth, 
    `Account Owner`, 
    Product, 
    State, 
    `Partner Success Owner`, 
    variableType, 
    ARRMonth

),

Filter_437_3118 AS (

  SELECT * 
  
  FROM Summarize_436_3118 AS in0
  
  WHERE (
          (NOT((((ARR = 0) OR (ARR IS NULL)) OR (ARR IS NULL)) OR ((LENGTH(CAST(ARR AS string))) = 0)))
          OR (((((ARR = 0) OR (ARR IS NULL)) OR (ARR IS NULL)) OR ((LENGTH(CAST(ARR AS string))) = 0)) IS NULL)
        )

),

AlteryxSelect_429_3118 AS (

  SELECT 
    CAST(Product AS string) AS Product,
    CAST(Sector AS string) AS Sector,
    CAST(variableType AS string) AS variableType,
    CAST(`Territory Name` AS string) AS `Territory Name`,
    CAST(State AS string) AS State,
    CAST(`Account Owner` AS string) AS `Account Owner`,
    CAST(`Partner Success Owner` AS string) AS `Partner Success Owner`,
    ARRMonth AS RevMonth,
    * EXCEPT (`Product`, 
    `Sector`, 
    `variableType`, 
    `Territory Name`, 
    `State`, 
    `Account Owner`, 
    `Partner Success Owner`, 
    `ARRMonth`)
  
  FROM Filter_437_3118 AS in0

),

Formula_431_3118_0 AS (

  SELECT 
    CAST((
      (
        CASE
          WHEN (((ARR / 0.01) < 0) AND (((ARR / 0.01) - FLOOR((ARR / 0.01))) = 0.5))
            THEN CEIL((ARR / 0.01))
          ELSE ROUND((ARR / 0.01))
        END
      )
      * 0.01
    ) AS DOUBLE) AS ARR,
    * EXCEPT (`arr`)
  
  FROM AlteryxSelect_429_3118 AS in0

),

Formula_431_3118_1 AS (

  SELECT 
    CAST((
      (
        CASE
          WHEN ((((ARR / 12) / 0.01) < 0) AND ((((ARR / 12) / 0.01) - FLOOR(((ARR / 12) / 0.01))) = 0.5))
            THEN CEIL(((ARR / 12) / 0.01))
          ELSE ROUND(((ARR / 12) / 0.01))
        END
      )
      * 0.01
    ) AS DOUBLE) AS MRR,
    (TO_DATE((DATE_TRUNC('month', RevMonth)), 'yyyy-MM-dd')) AS RevMonth,
    * EXCEPT (`revmonth`)
  
  FROM Formula_431_3118_0 AS in0

),

MultiFieldFormula_432_3118 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['Formula_431_3118_1'], 
      "CASE WHEN CAST(isnull(column_value) AS BOOLEAN) THEN 'Other' WHEN (isnull(column_value) OR (length(column_value) = 0)) THEN 'Other' WHEN (upper(column_value) = upper('N/A')) THEN 'Other' ELSE column_value END", 
      [
        'MRR', 
        'RevMonth', 
        'ARR', 
        'Product', 
        'Sector', 
        'variableType', 
        'Territory Name', 
        'State', 
        'Account Owner', 
        'Partner Success Owner', 
        'Quantity', 
        'StaticHistoryYearEnd', 
        'YetToRenewARR', 
        'CustomerName', 
        'MaxIteration', 
        'Account Size', 
        'StaticHistoryMonth'
      ], 
      ['Sector', 'variableType', 'Territory Name', 'State', 'Account Owner', 'Partner Success Owner'], 
      false, 
      'Suffix', 
      ''
    )
  }}

)

SELECT *

FROM MultiFieldFormula_432_3118
