{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_3342 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3342')}}

),

Filter_2558 AS (

  SELECT * 
  
  FROM AlteryxSelect_3342 AS in0
  
  WHERE (
          (
            NOT(
              (`Acquired ARR from Quorum (QualityAssist)` IS NULL)
              OR ((LENGTH(CAST(`Acquired ARR from Quorum (QualityAssist)` AS string))) = 0))
          )
          AND (NOT(`Acquired ARR from Quorum (QualityAssist)` IS NULL))
        )

),

AlteryxSelect_2559 AS (

  SELECT 
    `Mas90 Customer Number` AS CustomerName,
    Sector AS SubCustSeg1,
    `Territory Name` AS SubCustSeg3,
    variableType AS SubCustSeg2,
    `Partner Success Owner` AS SubCustSeg6,
    `Account Owner` AS SubCustSeg5,
    CAST(`Acquired ARR from Quorum (QualityAssist)` AS DOUBLE) AS Revenue
  
  FROM Filter_2558 AS in0

),

AlteryxSelect_2038 AS (

  SELECT 
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    CAST(`Acquired ARR from Quorum (QualityAssist)` AS DOUBLE) AS `Acquired ARR from Quorum (QualityAssist)`
  
  FROM AlteryxSelect_3342 AS in0

),

Filter_2039 AS (

  SELECT * 
  
  FROM AlteryxSelect_2038 AS in0
  
  WHERE (
          (
            NOT(
              (`Acquired ARR from Quorum (QualityAssist)` IS NULL)
              OR ((LENGTH(CAST(`Acquired ARR from Quorum (QualityAssist)` AS string))) = 0))
          )
          AND (NOT((`Mas90 Customer Number` IS NULL) OR ((LENGTH(`Mas90 Customer Number`)) = 0)))
        )

),

AlteryxSelect_2022 AS (

  SELECT 
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    CAST(`Acquired ARR from ReadyRosie` AS DOUBLE) AS `Acquired ARR from ReadyRosie`
  
  FROM AlteryxSelect_3342 AS in0

),

Union_1980 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_1980')}}

),

Formula_2560_0 AS (

  SELECT 
    (TO_TIMESTAMP('2022-01-01 00:00:00')) AS RevMonth,
    CAST(0 AS DOUBLE) AS YetToRenew,
    CAST('Quorum' AS string) AS Product,
    CAST(0 AS DOUBLE) AS Volume,
    CAST('0 classrooms' AS string) AS `Customer Segment`,
    *
  
  FROM AlteryxSelect_2559 AS in0

),

Filter_2568 AS (

  SELECT * 
  
  FROM Formula_2560_0 AS in0
  
  WHERE (
          (NOT(CAST(CustomerName AS string) IN ('07A232847', '07A233189', '07A233231', '07A233119')))
          OR (CustomerName IS NULL)
        )

),

Union_2567_reformat_0 AS (

  SELECT 
    `Customer Segment` AS `Customer Segment`,
    CustomerName AS CustomerName,
    Product AS Product,
    (TO_DATE(RevMonth, 'yyyy-MM-dd')) AS RevMonth,
    Revenue AS Revenue,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg2 AS SubCustSeg2,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    Volume AS Volume,
    YetToRenew AS YetToRenew
  
  FROM Filter_2568 AS in0

),

Filter_2023 AS (

  SELECT * 
  
  FROM AlteryxSelect_2022 AS in0
  
  WHERE (
          (
            NOT(
              (`Acquired ARR from ReadyRosie` IS NULL)
              OR ((LENGTH(CAST(`Acquired ARR from ReadyRosie` AS string))) = 0))
          )
          AND (NOT((`Mas90 Customer Number` IS NULL) OR ((LENGTH(`Mas90 Customer Number`)) = 0)))
        )

),

TextInput_1948_cast AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TextInput_1948_cast')}}

),

Join_1947_inner AS (

  SELECT 
    in0.CustomerName AS CustomerName,
    in1.`Clean Product` AS Product,
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
    in0.YetToRenewARR AS YetToRenewARR,
    in0.ChangeLog AS ChangeLog,
    in0.`Output Name` AS `Output Name`,
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
    `MRR`, 
    `YetToRenewARR`, 
    `ChangeLog`, 
    `Output Name`),
    in1.* EXCEPT (`Clean Product`, `Product`)
  
  FROM Union_1980 AS in0
  INNER JOIN TextInput_1948_cast AS in1
     ON (in0.Product = in1.Product)

),

Filter_2019_reject AS (

  SELECT * 
  
  FROM Join_1947_inner AS in0
  
  WHERE (
          (
            NOT(NOT(
              UPPER(Product) = UPPER('ReadyRosie')))
          )
          OR (
               (
                 NOT(
                   UPPER(Product) = UPPER('ReadyRosie'))
               ) IS NULL
             )
        )

),

Summarize_2020 AS (

  SELECT 
    MIN(RevMonth) AS Min_RevMonth,
    CustomerName AS CustomerName
  
  FROM Filter_2019_reject AS in0
  
  GROUP BY CustomerName

),

Join_2024_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CustomerName`, `Min_RevMonth`)
  
  FROM Filter_2019_reject AS in0
  INNER JOIN Summarize_2020 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.RevMonth = in1.Min_RevMonth))

),

Join_2025_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Mas90 Customer Number`)
  
  FROM Join_2024_inner AS in0
  INNER JOIN Filter_2023 AS in1
     ON (in0.CustomerName = in1.`Mas90 Customer Number`)

),

Formula_2026_0 AS (

  SELECT 
    (TO_DATE('2019-06-01', 'yyyy-MM-dd')) AS `Min Date`,
    *
  
  FROM Join_2025_inner AS in0

),

Filter_2027_reject AS (

  SELECT * 
  
  FROM Formula_2026_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  `Min Date` = RevMonth)
              ) OR (`Min Date` IS NULL)
            ) OR (RevMonth IS NULL)
          )
          OR ((`Min Date` = RevMonth) IS NULL)
        )

),

Formula_2029_0 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', (DATE_ADD(RevMonth, CAST(-1 AS INTEGER))))), 'yyyy-MM-dd')) AS `Max Date`,
    *
  
  FROM Filter_2027_reject AS in0

),

GenerateRows_2030 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Formula_2029_0'], 
      '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Min Date", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "Acquired ARR from ReadyRosie", "dataType": "Double"}, {"name": "Max Date", "dataType": "Date"}, {"name": "MRR", "dataType": "Double"}]', 
      'payload.`Min Date`', 
      '(`Acquired RevMonth` <= payload.`Max Date`)', 
      'add_months(`Acquired RevMonth`, 1)', 
      'Acquired RevMonth', 
      '100', 
      'recursive'
    )
  }}

),

Formula_2033_0 AS (

  SELECT 
    (LAST_DAY(CAST(`Acquired RevMonth` AS DATE))) AS StaticHistoryMonth,
    (
      TO_DATE(
        (
          CONCAT(
            (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST(EXTRACT(YEAR FROM `Acquired RevMonth`) AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            ), 
            '-12-31')
        ))
    ) AS StaticHistoryYearEnd,
    CAST(`Acquired ARR from ReadyRosie` AS DOUBLE) AS ARR,
    * EXCEPT (`statichistoryyearend`, `arr`, `statichistorymonth`)
  
  FROM GenerateRows_2030 AS in0

),

Formula_2033_1 AS (

  SELECT 
    CAST((ARR / 12) AS DOUBLE) AS MRR,
    * EXCEPT (`mrr`)
  
  FROM Formula_2033_0 AS in0

),

AlteryxSelect_2034 AS (

  SELECT 
    CustomerName AS CustomerName,
    Product AS Product,
    `Account Size` AS `Account Size`,
    Sector AS Sector,
    variableType AS variableType,
    `Territory Name` AS `Territory Name`,
    State AS State,
    `Account Owner` AS `Account Owner`,
    `Partner Success Owner` AS `Partner Success Owner`,
    `Acquired RevMonth` AS RevMonth,
    Quantity AS Quantity,
    ARR AS ARR,
    StaticHistoryMonth AS StaticHistoryMonth,
    MaxIteration AS MaxIteration,
    StaticHistoryYearEnd AS StaticHistoryYearEnd,
    MRR AS MRR,
    YetToRenewARR AS YetToRenewARR,
    ChangeLog AS ChangeLog,
    * EXCEPT (`RevMonth`, 
    `Acquired ARR from ReadyRosie`, 
    `Min Date`, 
    `Max Date`, 
    `CustomerName`, 
    `Product`, 
    `Account Size`, 
    `Sector`, 
    `variableType`, 
    `Territory Name`, 
    `State`, 
    `Account Owner`, 
    `Partner Success Owner`, 
    `Quantity`, 
    `ARR`, 
    `StaticHistoryMonth`, 
    `MaxIteration`, 
    `StaticHistoryYearEnd`, 
    `MRR`, 
    `YetToRenewARR`, 
    `ChangeLog`, 
    `Acquired RevMonth`)
  
  FROM Formula_2033_1 AS in0

),

Join_2025_left AS (

  SELECT in0.*
  
  FROM Join_2024_inner AS in0
  ANTI JOIN Filter_2023 AS in1
     ON (in0.CustomerName = in1.`Mas90 Customer Number`)

),

Join_2024_left AS (

  SELECT in0.*
  
  FROM Filter_2019_reject AS in0
  ANTI JOIN Summarize_2020 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.RevMonth = in1.Min_RevMonth))

),

Filter_2019 AS (

  SELECT * 
  
  FROM Join_1947_inner AS in0
  
  WHERE (
          NOT(
            UPPER(Product) = UPPER('ReadyRosie'))
        )

),

AlteryxSelect_2032 AS (

  SELECT * EXCEPT (`Acquired ARR from ReadyRosie`, `Min Date`)
  
  FROM Filter_2027_reject AS in0

),

Filter_2027 AS (

  SELECT * 
  
  FROM Formula_2026_0 AS in0
  
  WHERE (`Min Date` = RevMonth)

),

AlteryxSelect_2031 AS (

  SELECT * EXCEPT (`Acquired ARR from ReadyRosie`, `Min Date`)
  
  FROM Filter_2027 AS in0

),

Union_2028 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'AlteryxSelect_2032', 
        'Filter_2019', 
        'Join_2024_left', 
        'Join_2025_left', 
        'AlteryxSelect_2034', 
        'AlteryxSelect_2031'
      ], 
      [
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_2035_reject AS (

  SELECT * 
  
  FROM Union_2028 AS in0
  
  WHERE (
          (
            NOT(NOT(
              UPPER(Product) = UPPER('Quorum')))
          )
          OR (
               (
                 NOT(
                   UPPER(Product) = UPPER('Quorum'))
               ) IS NULL
             )
        )

),

Summarize_2036 AS (

  SELECT 
    MIN(RevMonth) AS Min_RevMonth,
    CustomerName AS CustomerName
  
  FROM Filter_2035_reject AS in0
  
  GROUP BY CustomerName

),

Join_2040_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CustomerName`, `Min_RevMonth`)
  
  FROM Filter_2035_reject AS in0
  INNER JOIN Summarize_2036 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.RevMonth = in1.Min_RevMonth))

),

Join_2041_left AS (

  SELECT in0.*
  
  FROM Join_2040_inner AS in0
  ANTI JOIN Filter_2039 AS in1
     ON (in0.CustomerName = in1.`Mas90 Customer Number`)

),

Join_2040_left AS (

  SELECT in0.*
  
  FROM Filter_2035_reject AS in0
  ANTI JOIN Summarize_2036 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.RevMonth = in1.Min_RevMonth))

),

Join_2041_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Mas90 Customer Number`)
  
  FROM Join_2040_inner AS in0
  INNER JOIN Filter_2039 AS in1
     ON (in0.CustomerName = in1.`Mas90 Customer Number`)

),

Formula_2042_0 AS (

  SELECT 
    (TO_DATE('2022-02-01', 'yyyy-MM-dd')) AS `Min Date`,
    *
  
  FROM Join_2041_inner AS in0

),

Filter_2043 AS (

  SELECT * 
  
  FROM Formula_2042_0 AS in0
  
  WHERE (`Min Date` = RevMonth)

),

AlteryxSelect_2047 AS (

  SELECT * EXCEPT (`Acquired ARR from Quorum (QualityAssist)`, `Min Date`)
  
  FROM Filter_2043 AS in0

),

Filter_2043_reject AS (

  SELECT * 
  
  FROM Formula_2042_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  `Min Date` = RevMonth)
              ) OR (`Min Date` IS NULL)
            ) OR (RevMonth IS NULL)
          )
          OR ((`Min Date` = RevMonth) IS NULL)
        )

),

AlteryxSelect_2048 AS (

  SELECT * EXCEPT (`Acquired ARR from Quorum (QualityAssist)`, `Min Date`)
  
  FROM Filter_2043_reject AS in0

),

Filter_2035 AS (

  SELECT * 
  
  FROM Union_2028 AS in0
  
  WHERE (
          NOT(
            UPPER(Product) = UPPER('Quorum'))
        )

),

Formula_2045_0 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', (DATE_ADD(RevMonth, CAST(-1 AS INTEGER))))), 'yyyy-MM-dd')) AS `Max Date`,
    *
  
  FROM Filter_2043_reject AS in0

),

GenerateRows_2046 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Formula_2045_0'], 
      '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Acquired ARR from Quorum (QualityAssist)", "dataType": "Double"}, {"name": "Min Date", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "Max Date", "dataType": "Date"}, {"name": "MRR", "dataType": "Double"}]', 
      'payload.`Min Date`', 
      '(`Acquired RevMonth` <= payload.`Max Date`)', 
      'add_months(`Acquired RevMonth`, 1)', 
      'Acquired RevMonth', 
      '100', 
      'recursive'
    )
  }}

),

Formula_2049_0 AS (

  SELECT 
    (LAST_DAY(CAST(`Acquired RevMonth` AS DATE))) AS StaticHistoryMonth,
    (
      TO_DATE(
        (
          CONCAT(
            (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST(EXTRACT(YEAR FROM `Acquired RevMonth`) AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            ), 
            '-12-31')
        ))
    ) AS StaticHistoryYearEnd,
    CAST(`Acquired ARR from Quorum (QualityAssist)` AS DOUBLE) AS ARR,
    * EXCEPT (`statichistoryyearend`, `arr`, `statichistorymonth`)
  
  FROM GenerateRows_2046 AS in0

),

Formula_2049_1 AS (

  SELECT 
    CAST((ARR / 12) AS DOUBLE) AS MRR,
    * EXCEPT (`mrr`)
  
  FROM Formula_2049_0 AS in0

),

AlteryxSelect_2050 AS (

  SELECT 
    CustomerName AS CustomerName,
    Product AS Product,
    `Account Size` AS `Account Size`,
    Sector AS Sector,
    variableType AS variableType,
    `Territory Name` AS `Territory Name`,
    State AS State,
    `Account Owner` AS `Account Owner`,
    `Partner Success Owner` AS `Partner Success Owner`,
    `Acquired RevMonth` AS RevMonth,
    Quantity AS Quantity,
    ARR AS ARR,
    StaticHistoryMonth AS StaticHistoryMonth,
    MaxIteration AS MaxIteration,
    StaticHistoryYearEnd AS StaticHistoryYearEnd,
    MRR AS MRR,
    YetToRenewARR AS YetToRenewARR,
    ChangeLog AS ChangeLog,
    * EXCEPT (`RevMonth`, 
    `Min Date`, 
    `Max Date`, 
    `Acquired ARR from Quorum (QualityAssist)`, 
    `CustomerName`, 
    `Product`, 
    `Account Size`, 
    `Sector`, 
    `variableType`, 
    `Territory Name`, 
    `State`, 
    `Account Owner`, 
    `Partner Success Owner`, 
    `Quantity`, 
    `ARR`, 
    `StaticHistoryMonth`, 
    `MaxIteration`, 
    `StaticHistoryYearEnd`, 
    `MRR`, 
    `YetToRenewARR`, 
    `ChangeLog`, 
    `Acquired RevMonth`)
  
  FROM Formula_2049_1 AS in0

),

Union_2044 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Filter_2035', 
        'AlteryxSelect_2050', 
        'AlteryxSelect_2048', 
        'AlteryxSelect_2047', 
        'Join_2041_left', 
        'Join_2040_left'
      ], 
      [
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Arrange_1204_consolidatedDataCol_0 AS (

  SELECT 
    (
      ARRAY(
        (
          NAMED_STRUCT(
            'CustomerName', 
            CAST(CustomerName AS string), 
            'RevMonth', 
            CAST(RevMonth AS string), 
            'Revenue', 
            CAST(ARR AS string), 
            'YetToRenew', 
            CAST(YetToRenewARR AS string), 
            'Product', 
            CAST(Product AS string), 
            'Customer Segment', 
            CAST(`Account Size` AS string), 
            'SubCustSeg1', 
            CAST(Sector AS string), 
            'SubCustSeg2', 
            CAST(variableType AS string), 
            'SubCustSeg3', 
            CAST(`Territory Name` AS string), 
            'SubCustSeg4', 
            CAST(State AS string), 
            'SubCustSeg5', 
            CAST(`Account Owner` AS string), 
            'SubCustSeg6', 
            CAST(`Partner Success Owner` AS string), 
            'Volume', 
            CAST(Quantity AS string), 
            'Description', 
            '')
        ))
    ) AS _consolidated_data_col,
    *
  
  FROM Union_2044 AS in0

),

Arrange_1204_explode_0 AS (

  SELECT 
    (EXPLODE(_consolidated_data_col)) AS _exploded_data_col,
    *
  
  FROM Arrange_1204_consolidatedDataCol_0 AS in0

),

Arrange_1204_0 AS (

  SELECT 
    _exploded_data_col.`CustomerName` AS CustomerName,
    _exploded_data_col.`RevMonth` AS RevMonth,
    _exploded_data_col.`Revenue` AS Revenue,
    _exploded_data_col.`YetToRenew` AS YetToRenew,
    _exploded_data_col.`Product` AS Product,
    _exploded_data_col.`Customer Segment` AS `Customer Segment`,
    _exploded_data_col.`SubCustSeg1` AS SubCustSeg1,
    _exploded_data_col.`SubCustSeg2` AS SubCustSeg2,
    _exploded_data_col.`SubCustSeg3` AS SubCustSeg3,
    _exploded_data_col.`SubCustSeg4` AS SubCustSeg4,
    _exploded_data_col.`SubCustSeg5` AS SubCustSeg5,
    _exploded_data_col.`SubCustSeg6` AS SubCustSeg6,
    _exploded_data_col.`Volume` AS Volume,
    _exploded_data_col.`Description` AS Description,
    * EXCEPT (`customername`, `revmonth`, `product`)
  
  FROM Arrange_1204_explode_0 AS in0

),

Arrange_1204_selectCols AS (

  SELECT * 
  
  FROM Arrange_1204_0 AS in0

),

Filter_2565 AS (

  SELECT * 
  
  FROM Arrange_1204_selectCols AS in0
  
  WHERE (
          (
            (CAST(CustomerName AS STRING) IN ('07A232847', '07A233189', '07A233231', '07A233119'))
            AND (upper(Product) = upper('Quorum'))
          )
          AND (RevMonth = to_date('2022-01-01 00:00:00'))
        )

),

Join_2566_inner AS (

  SELECT 
    in0.* EXCEPT (`Revenue`),
    in1.* EXCEPT (`CustomerName`, `SubCustSeg1`, `SubCustSeg3`, `SubCustSeg2`, `SubCustSeg6`, `SubCustSeg5`)
  
  FROM Filter_2565 AS in0
  INNER JOIN AlteryxSelect_2559 AS in1
     ON (in0.CustomerName = in1.CustomerName)

),

Union_2567_reformat_2 AS (

  SELECT 
    `Customer Segment` AS `Customer Segment`,
    CustomerName AS CustomerName,
    CAST(Description AS string) AS Description,
    Product AS Product,
    (TO_DATE(RevMonth, 'yyyy-MM-dd')) AS RevMonth,
    Revenue AS Revenue,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg2 AS SubCustSeg2,
    SubCustSeg3 AS SubCustSeg3,
    CAST(SubCustSeg4 AS string) AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    Volume AS Volume,
    YetToRenew AS YetToRenew
  
  FROM Join_2566_inner AS in0

),

Filter_2565_reject AS (

  SELECT * 
  
  FROM Arrange_1204_selectCols AS in0
  
  WHERE (
          NOT (
            (
              (
                (CAST(CustomerName AS STRING) IN ('07A232847', '07A233189', '07A233231', '07A233119'))
                AND (upper(Product) = upper('Quorum'))
              )
              AND (RevMonth = to_date('2022-01-01 00:00:00'))
            )
          )
          OR isnull(
               (
                 (
                   (CAST(CustomerName AS STRING) IN ('07A232847', '07A233189', '07A233231', '07A233119'))
                   AND (upper(Product) = upper('Quorum'))
                 )
                 AND (RevMonth = to_date('2022-01-01 00:00:00'))
               ))
        )

),

Union_2567_reformat_1 AS (

  SELECT 
    `Customer Segment` AS `Customer Segment`,
    CustomerName AS CustomerName,
    CAST(Description AS string) AS Description,
    Product AS Product,
    (TO_DATE(RevMonth, 'yyyy-MM-dd')) AS RevMonth,
    Revenue AS Revenue,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg2 AS SubCustSeg2,
    SubCustSeg3 AS SubCustSeg3,
    CAST(SubCustSeg4 AS string) AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    Volume AS Volume,
    YetToRenew AS YetToRenew
  
  FROM Filter_2565_reject AS in0

),

Union_2567 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_2567_reformat_1', 'Union_2567_reformat_2', 'Union_2567_reformat_0'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Description", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Description", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "RevMonth", "dataType": "Timestamp"}, {"name": "Revenue", "dataType": "Double"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_1668 AS (

  SELECT 
    SUM(Revenue) AS Revenue,
    SUM(YetToRenew) AS YetToRenew,
    SUM(Volume) AS Volume,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg1 AS SubCustSeg1,
    CustomerName AS CustomerName,
    SubCustSeg6 AS SubCustSeg6,
    `Customer Segment` AS `Customer Segment`,
    RevMonth AS RevMonth,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    Product AS Product,
    SubCustSeg2 AS SubCustSeg2
  
  FROM Union_2567 AS in0
  
  GROUP BY 
    SubCustSeg5, 
    SubCustSeg1, 
    CustomerName, 
    SubCustSeg6, 
    `Customer Segment`, 
    RevMonth, 
    SubCustSeg3, 
    SubCustSeg4, 
    Product, 
    SubCustSeg2

),

Formula_1183 AS (

  SELECT *
  
  FROM Summarize_1668 AS in0

)

SELECT *

FROM Formula_1183
