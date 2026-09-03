{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_1972_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1972_0')}}

),

Summarize_1974 AS (

  SELECT DISTINCT StaticHistoryYearEnd AS StaticHistoryYearEnd
  
  FROM Formula_1972_0 AS in0

),

AlteryxSelect_1894 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_1894')}}

),

AppendFields_1968 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_1974 AS in0
  INNER JOIN AlteryxSelect_1894 AS in1
     ON TRUE

),

Filter_1970 AS (

  SELECT * 
  
  FROM AppendFields_1968 AS in0
  
  WHERE (`ARR Period` <= StaticHistoryYearEnd)

),

Filter_1969_reject AS (

  SELECT * 
  
  FROM Filter_1970 AS in0
  
  WHERE (
          (isnull(Stage) OR isnull(not(isnull(Stage))))
          AND (
                NOT (((upper(Origin) = upper('Orders&OrdersProcessed')) AND (`ARR Period` > to_date(StaticHistoryMonth))))
                OR isnull(
                     ((upper(Origin) = upper('Orders&OrdersProcessed')) AND (`ARR Period` > to_date(StaticHistoryMonth))))
              )
        )

),

Filter_1969 AS (

  SELECT * 
  
  FROM Filter_1970 AS in0
  
  WHERE (
          NOT (isnull(Stage))
          AND (
                NOT (((upper(Origin) = upper('Orders&OrdersProcessed')) AND (`ARR Period` > to_date(StaticHistoryMonth))))
                OR isnull(
                     ((upper(Origin) = upper('Orders&OrdersProcessed')) AND (`ARR Period` > to_date(StaticHistoryMonth))))
              )
        )

),

RecordID_1986 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_1969'], 
      'incremental_id', 
      'RecordID2', 
      'integer', 
      6, 
      1, 
      'groupLevel', 
      'first_column', 
      [], 
      [{ 'expression': { 'expression': 'RecordID' }, 'sortType': 'asc' }]
    )
  }}

),

Formula_1971_0 AS (

  SELECT 
    CAST(CASE
      WHEN (`Open Renewal Flag` = 0)
        THEN 0
      ELSE TCV
    END AS DOUBLE) AS TCV,
    * EXCEPT (`tcv`)
  
  FROM RecordID_1986 AS in0

),

AlteryxSelect_1987 AS (

  SELECT * EXCEPT (`RecordID`)
  
  FROM Formula_1971_0 AS in0

),

Union_2303 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_1969_reject', 'AlteryxSelect_1987'], 
      [
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "RecordID2", "dataType": "Integer"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_2298 AS (

  SELECT * 
  
  FROM Union_2303 AS in0
  
  WHERE ((NOT(ContractStartDate IS NULL)) AND (NOT(ContractEndDate IS NULL)))

),

Formula_2301_0 AS (

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
  
  FROM Filter_2298 AS in0

),

Formula_2301_1 AS (

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
    ) AS DOUBLE) AS ARR,
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
    *
  
  FROM Formula_2301_0 AS in0

),

Formula_2301_2 AS (

  SELECT 
    CAST((
      CASE
        WHEN (ARR < 0)
          THEN 0
        ELSE ARR
      END
    ) AS DOUBLE) AS ARR,
    * EXCEPT (`arr`)
  
  FROM Formula_2301_1 AS in0

),

GenerateRows_2300 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Formula_2301_2'], 
      '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "RecordID2", "dataType": "Integer"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "ContractTermMonths", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]', 
      'last_day(payload.ContractStartDate)', 
      '((ARRMonth <= payload.ContractEndDate) AND (ARRMonth <= concat(regexp_replace(regexp_replace(format_number(CAST(year(current_timestamp()) AS DOUBLE), 0), ",", "__THS__"), "__THS__", ""), "-12-31")))', 
      'last_day(add_months(ARRMonth, 1))', 
      'ARRMonth', 
      '100', 
      'recursive'
    )
  }}

),

Filter_1990_to_Filter_1995 AS (

  SELECT * 
  
  FROM GenerateRows_2300 AS in0
  
  WHERE (
          (
            (ARRMonth >= ContractStartDate)
            AND (ARRMonth < to_date(substring(CAST(date_add(ContractEndDate, CAST(1 AS INT)) AS STRING), 1, 10)))
          )
          AND (
                NOT (coalesce(contains(lower(Stage), lower('Closed')), false))
                OR (`Actual Closed Date` > to_date(StaticHistoryMonth))
              )
        )

),

Summarize_1992 AS (

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
  
  FROM Filter_1990_to_Filter_1995 AS in0
  
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

AppendFields_1996 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_1974 AS in0
  INNER JOIN Summarize_1992 AS in1
     ON TRUE

),

Filter_1989 AS (

  SELECT * 
  
  FROM Formula_1971_0 AS in0
  
  WHERE CAST(`Open Renewal Flag` AS BOOLEAN)

),

Join_1988_inner AS (

  SELECT 
    in1.Engine_ContractDays AS Right_Engine_ContractDays,
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
    `StaticHistoryYearEnd`, 
    `RecordID2`),
    in1.* EXCEPT (`Engine_ContractDays`)
  
  FROM Filter_1989 AS in0
  INNER JOIN RecordID_1986 AS in1
     ON (in0.RecordID = in1.RecordID)

),

Filter_1970_reject AS (

  SELECT * 
  
  FROM AppendFields_1968 AS in0
  
  WHERE (
          (
            NOT(
              `ARR Period` <= StaticHistoryYearEnd)
          )
          OR ((`ARR Period` <= StaticHistoryYearEnd) IS NULL)
        )

),

Union_1985 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_1970_reject', 'Join_1988_inner'], 
      [
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "RecordID2", "dataType": "Integer"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "Right_Engine_ContractDays", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_1981 AS (

  SELECT * 
  
  FROM Union_1985 AS in0
  
  WHERE (`Created Date` <= to_date(StaticHistoryMonth))

),

Formula_1982_0 AS (

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
  
  FROM Filter_1981 AS in0

),

Formula_1982_1 AS (

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
  
  FROM Formula_1982_0 AS in0

),

Formula_1982_2 AS (

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
  
  FROM Formula_1982_1 AS in0

),

GenerateRows_1983 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Formula_1982_2'], 
      '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "RecordID2", "dataType": "Integer"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Right_Engine_ContractDays", "dataType": "Double"}, {"name": "YetToRenewStart", "dataType": "Date"}, {"name": "ContractTermMonths", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "YetToRenewEnd", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]', 
      'last_day(payload.YetToRenewStart)', 
      '((YetToRenewMonth <= payload.YetToRenewEnd) AND (YetToRenewMonth <= concat(regexp_replace(regexp_replace(format_number(CAST(year(current_timestamp()) AS DOUBLE), 0), ",", "__THS__"), "__THS__", ""), "-12-31")))', 
      'last_day(add_months(YetToRenewMonth, 1))', 
      'YetToRenewMonth', 
      '100', 
      'recursive'
    )
  }}

),

Summarize_1984 AS (

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
  
  FROM GenerateRows_1983 AS in0
  
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

Join_1999_left_UnionLeftOuter AS (

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
  
  FROM AppendFields_1996 AS in0
  LEFT JOIN Summarize_1984 AS in1
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

Filter_1994 AS (

  SELECT * 
  
  FROM Join_1999_left_UnionLeftOuter AS in0
  
  WHERE (ARRMonth <= StaticHistoryYearEnd)

),

RecordID_1956 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_1994'], 
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

Formula_2057_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((Quantity IS NULL) AS BOOLEAN)
          THEN 0
        ELSE Quantity
      END
    ) AS DOUBLE) AS Quantity,
    * EXCEPT (`quantity`)
  
  FROM RecordID_1956 AS in0

),

Filter_1965 AS (

  SELECT * 
  
  FROM Formula_2057_0 AS in0
  
  WHERE (UPPER(Product) = UPPER('CC Cloud'))

),

Summarize_1959 AS (

  SELECT 
    SUM(Quantity) AS Quantity,
    CustomerName AS CustomerName,
    ARRMonth AS ARRMonth
  
  FROM Filter_1965 AS in0
  
  GROUP BY 
    CustomerName, ARRMonth

),

Formula_1960_0 AS (

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
  
  FROM Summarize_1959 AS in0

),

Join_1961_inner_UnionLeftOuter AS (

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
  
  FROM RecordID_1956 AS in0
  LEFT JOIN Formula_1960_0 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.ARRMonth = in1.ARRMonth))

),

Formula_1964_0 AS (

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
  
  FROM Join_1961_inner_UnionLeftOuter AS in0

),

Summarize_1962 AS (

  SELECT 
    SUM(Quantity) AS Quantity,
    SUM(ARR) AS ARR,
    StaticHistoryYearEnd AS StaticHistoryYearEnd,
    YetToRenewARR AS YetToRenewARR,
    Sector AS Sector,
    `Territory Name` AS `Territory Name`,
    CustomerName AS CustomerName,
    `Account Size` AS `Account Size`,
    `Account Owner` AS `Account Owner`,
    Product AS Product,
    State AS State,
    `Partner Success Owner` AS `Partner Success Owner`,
    variableType AS variableType,
    ARRMonth AS ARRMonth
  
  FROM Formula_1964_0 AS in0
  
  GROUP BY 
    StaticHistoryYearEnd, 
    YetToRenewARR, 
    Sector, 
    `Territory Name`, 
    CustomerName, 
    `Account Size`, 
    `Account Owner`, 
    Product, 
    State, 
    `Partner Success Owner`, 
    variableType, 
    ARRMonth

),

Filter_1963 AS (

  SELECT * 
  
  FROM Summarize_1962 AS in0
  
  WHERE (
          (NOT((((ARR = 0) OR (ARR IS NULL)) OR (ARR IS NULL)) OR ((LENGTH(CAST(ARR AS string))) = 0)))
          OR (((((ARR = 0) OR (ARR IS NULL)) OR (ARR IS NULL)) OR ((LENGTH(CAST(ARR AS string))) = 0)) IS NULL)
        )

),

AlteryxSelect_1955 AS (

  SELECT 
    CAST(Product AS string) AS Product,
    ARRMonth AS RevMonth,
    * EXCEPT (`Product`, `ARRMonth`)
  
  FROM Filter_1963 AS in0

),

Formula_1957_0 AS (

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
  
  FROM AlteryxSelect_1955 AS in0

),

Formula_1957_1 AS (

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
    *
  
  FROM Formula_1957_0 AS in0

),

MultiFieldFormula_1958 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['Formula_1957_1'], 
      "CASE WHEN CAST(isnull(column_value) AS BOOLEAN) THEN 'Other' WHEN (isnull(column_value) OR (length(column_value) = 0)) THEN 'Other' WHEN (upper(column_value) = upper('N/A')) THEN 'Other' ELSE column_value END", 
      [
        'StaticHistoryYearEnd', 
        'Quantity', 
        'YetToRenewARR', 
        'Sector', 
        'Territory Name', 
        'CustomerName', 
        'variableType', 
        'RevMonth', 
        'ARR', 
        'Account Size', 
        'Account Owner', 
        'Product', 
        'State', 
        'Partner Success Owner', 
        'MRR'
      ], 
      ['Sector', 'variableType', 'Territory Name', 'State', 'Account Owner', 'Partner Success Owner'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Formula_2007_0 AS (

  SELECT 
    (LAST_DAY(CAST(RevMonth AS DATE))) AS RevMonth,
    * EXCEPT (`revmonth`)
  
  FROM MultiFieldFormula_1958 AS in0

),

Filter_2005 AS (

  SELECT * 
  
  FROM Formula_2007_0 AS in0
  
  WHERE (RevMonth = to_date({{ var('User__Current_Period') }}))

),

Formula_2006_0 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', RevMonth)), 'yyyy-MM-dd')) AS RevMonth,
    * EXCEPT (`revmonth`)
  
  FROM Filter_2005 AS in0

)

SELECT *

FROM Formula_2006_0
