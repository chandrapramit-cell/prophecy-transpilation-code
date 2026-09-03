{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH QualityAssist_O_3163 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'QualityAssist_O_3163'
    )
  }}

),

Formula_3186_0 AS (

  SELECT 
    CAST(CASE
      WHEN CAST(isnull(YEAR (variableDate)) AS BOOLEAN)
        THEN 1
      ELSE 0
    END AS BOOLEAN) AS `Date Check`,
    *
  
  FROM QualityAssist_O_3163 AS in0

),

Filter_3187_reject_to_Filter_3188 AS (

  SELECT * 
  
  FROM Formula_3186_0 AS in0
  
  WHERE (
          ((NOT(NOT CAST(`Date Check` AS BOOLEAN))) OR ((NOT CAST(`Date Check` AS BOOLEAN)) IS NULL))
          AND (NOT((variableDate IS NULL) OR ((LENGTH(variableDate)) = 0)))
        )

),

DateTime_3189_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(variableDate, 'MM/dd/yyyy')), 'yyyy-MM-dd')) AS `Invoice Date`,
    *
  
  FROM Filter_3187_reject_to_Filter_3188 AS in0

),

Filter_3194_reject AS (

  SELECT * 
  
  FROM DateTime_3189_0 AS in0
  
  WHERE (
          ((`Invoice Date` IS NULL) OR ((LENGTH(`Invoice Date`)) = 0))
          OR ((NOT((`Invoice Date` IS NULL) OR ((LENGTH(`Invoice Date`)) = 0))) IS NULL)
        )

),

AlteryxSelect_3197 AS (

  SELECT * EXCEPT (`Invoice Date`)
  
  FROM Filter_3194_reject AS in0

),

DateTime_3195_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(variableDate, 'dd-MMM-yyyy')), 'yyyy-MM-dd')) AS `Invoice Date`,
    *
  
  FROM AlteryxSelect_3197 AS in0

),

Filter_3196 AS (

  SELECT * 
  
  FROM DateTime_3195_0 AS in0
  
  WHERE (NOT((`Invoice Date` IS NULL) OR ((LENGTH(`Invoice Date`)) = 0)))

),

Filter_3187 AS (

  SELECT * 
  
  FROM Formula_3186_0 AS in0
  
  WHERE (NOT CAST(`Date Check` AS BOOLEAN))

),

AlteryxSelect_3193 AS (

  SELECT 
    variableDate AS `Invoice Date`,
    * EXCEPT (`variableDate`)
  
  FROM Filter_3187 AS in0

),

Filter_3194 AS (

  SELECT * 
  
  FROM DateTime_3189_0 AS in0
  
  WHERE (NOT((`Invoice Date` IS NULL) OR ((LENGTH(`Invoice Date`)) = 0)))

),

Union_3198 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_3193', 'Filter_3194', 'Filter_3196'], 
      [
        '[{"name": "Invoice Date", "dataType": "String"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "2023-05-01", "dataType": "Double"}, {"name": "Term", "dataType": "String"}, {"name": "2021-06-01", "dataType": "Double"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2024-07-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "Name", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2022-02-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "Last Month", "dataType": "Date"}, {"name": "Monthly Revenue", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2021-07-01", "dataType": "Double"}, {"name": "2024-10-01", "dataType": "Double"}, {"name": "2024-01-01", "dataType": "Double"}, {"name": "2023-12-01", "dataType": "Double"}, {"name": "2023-02-01", "dataType": "Double"}, {"name": "2024-05-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2021-09-01", "dataType": "Double"}, {"name": "2023-08-01", "dataType": "Double"}, {"name": "2022-04-01", "dataType": "Double"}, {"name": "variableType", "dataType": "String"}, {"name": "2021-08-01", "dataType": "Double"}, {"name": "F10", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}, {"name": "2024-04-01", "dataType": "Double"}, {"name": "2023-09-01", "dataType": "Double"}, {"name": "First Month Revenue", "dataType": "Double"}, {"name": "2023-11-01", "dataType": "Double"}, {"name": "2021-05-01", "dataType": "Double"}, {"name": "2022-03-01", "dataType": "Double"}, {"name": "2023-10-01", "dataType": "Double"}, {"name": "End Date", "dataType": "String"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "Last Month Revenue", "dataType": "Double"}, {"name": "2022-06-01", "dataType": "Double"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "Item2", "dataType": "String"}, {"name": "Amount", "dataType": "Double"}, {"name": "2024-03-01", "dataType": "Double"}, {"name": "2023-06-01", "dataType": "Double"}, {"name": "2022-12-01", "dataType": "Double"}, {"name": "2024-08-01", "dataType": "Double"}, {"name": "2023-04-01", "dataType": "Double"}, {"name": "CHECK", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "First Month", "dataType": "Date"}, {"name": "2023-07-01", "dataType": "Double"}, {"name": "2021-12-01", "dataType": "Double"}, {"name": "Start Date", "dataType": "String"}, {"name": "2022-01-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2022-08-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2022-10-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "Item", "dataType": "String"}, {"name": "Memo", "dataType": "String"}, {"name": "2024-12-01", "dataType": "Double"}, {"name": "2023-01-01", "dataType": "Double"}, {"name": "2021-04-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2021-11-01", "dataType": "Double"}, {"name": "2022-11-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Term2", "dataType": "String"}, {"name": "2024-09-01", "dataType": "Double"}, {"name": "2024-06-01", "dataType": "Double"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2023-03-01", "dataType": "Double"}, {"name": "2022-09-01", "dataType": "Double"}, {"name": "2022-07-01", "dataType": "Double"}, {"name": "2021-03-01", "dataType": "Double"}, {"name": "2024-11-01", "dataType": "Double"}, {"name": "Qty", "dataType": "Double"}, {"name": "Sales Price", "dataType": "String"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "2021-02-01", "dataType": "Double"}, {"name": "2022-05-01", "dataType": "Double"}, {"name": "2024-02-01", "dataType": "Double"}, {"name": "2021-10-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}]', 
        '[{"name": "Invoice Date", "dataType": "String"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "2023-05-01", "dataType": "Double"}, {"name": "Term", "dataType": "String"}, {"name": "2021-06-01", "dataType": "Double"}, {"name": "variableDate", "dataType": "String"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2024-07-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "Name", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2022-02-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "Last Month", "dataType": "Date"}, {"name": "Monthly Revenue", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2021-07-01", "dataType": "Double"}, {"name": "2024-10-01", "dataType": "Double"}, {"name": "2024-01-01", "dataType": "Double"}, {"name": "2023-12-01", "dataType": "Double"}, {"name": "2023-02-01", "dataType": "Double"}, {"name": "2024-05-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2021-09-01", "dataType": "Double"}, {"name": "2023-08-01", "dataType": "Double"}, {"name": "2022-04-01", "dataType": "Double"}, {"name": "variableType", "dataType": "String"}, {"name": "2021-08-01", "dataType": "Double"}, {"name": "F10", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}, {"name": "2024-04-01", "dataType": "Double"}, {"name": "2023-09-01", "dataType": "Double"}, {"name": "First Month Revenue", "dataType": "Double"}, {"name": "2023-11-01", "dataType": "Double"}, {"name": "2021-05-01", "dataType": "Double"}, {"name": "2022-03-01", "dataType": "Double"}, {"name": "2023-10-01", "dataType": "Double"}, {"name": "End Date", "dataType": "String"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "Last Month Revenue", "dataType": "Double"}, {"name": "2022-06-01", "dataType": "Double"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "Item2", "dataType": "String"}, {"name": "Amount", "dataType": "Double"}, {"name": "2024-03-01", "dataType": "Double"}, {"name": "2023-06-01", "dataType": "Double"}, {"name": "2022-12-01", "dataType": "Double"}, {"name": "2024-08-01", "dataType": "Double"}, {"name": "2023-04-01", "dataType": "Double"}, {"name": "CHECK", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "First Month", "dataType": "Date"}, {"name": "2023-07-01", "dataType": "Double"}, {"name": "2021-12-01", "dataType": "Double"}, {"name": "Start Date", "dataType": "String"}, {"name": "2022-01-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2022-08-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2022-10-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "Item", "dataType": "String"}, {"name": "Memo", "dataType": "String"}, {"name": "2024-12-01", "dataType": "Double"}, {"name": "2023-01-01", "dataType": "Double"}, {"name": "2021-04-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2021-11-01", "dataType": "Double"}, {"name": "2022-11-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Term2", "dataType": "String"}, {"name": "2024-09-01", "dataType": "Double"}, {"name": "2024-06-01", "dataType": "Double"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2023-03-01", "dataType": "Double"}, {"name": "2022-09-01", "dataType": "Double"}, {"name": "2022-07-01", "dataType": "Double"}, {"name": "2021-03-01", "dataType": "Double"}, {"name": "2024-11-01", "dataType": "Double"}, {"name": "Qty", "dataType": "Double"}, {"name": "Sales Price", "dataType": "String"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "2021-02-01", "dataType": "Double"}, {"name": "2022-05-01", "dataType": "Double"}, {"name": "2024-02-01", "dataType": "Double"}, {"name": "2021-10-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}]', 
        '[{"name": "Invoice Date", "dataType": "String"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "2023-05-01", "dataType": "Double"}, {"name": "Term", "dataType": "String"}, {"name": "2021-06-01", "dataType": "Double"}, {"name": "variableDate", "dataType": "String"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2024-07-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "Name", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2022-02-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "Last Month", "dataType": "Date"}, {"name": "Monthly Revenue", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2021-07-01", "dataType": "Double"}, {"name": "2024-10-01", "dataType": "Double"}, {"name": "2024-01-01", "dataType": "Double"}, {"name": "2023-12-01", "dataType": "Double"}, {"name": "2023-02-01", "dataType": "Double"}, {"name": "2024-05-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2021-09-01", "dataType": "Double"}, {"name": "2023-08-01", "dataType": "Double"}, {"name": "2022-04-01", "dataType": "Double"}, {"name": "variableType", "dataType": "String"}, {"name": "2021-08-01", "dataType": "Double"}, {"name": "F10", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}, {"name": "2024-04-01", "dataType": "Double"}, {"name": "2023-09-01", "dataType": "Double"}, {"name": "First Month Revenue", "dataType": "Double"}, {"name": "2023-11-01", "dataType": "Double"}, {"name": "2021-05-01", "dataType": "Double"}, {"name": "2022-03-01", "dataType": "Double"}, {"name": "2023-10-01", "dataType": "Double"}, {"name": "End Date", "dataType": "String"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "Last Month Revenue", "dataType": "Double"}, {"name": "2022-06-01", "dataType": "Double"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "Item2", "dataType": "String"}, {"name": "Amount", "dataType": "Double"}, {"name": "2024-03-01", "dataType": "Double"}, {"name": "2023-06-01", "dataType": "Double"}, {"name": "2022-12-01", "dataType": "Double"}, {"name": "2024-08-01", "dataType": "Double"}, {"name": "2023-04-01", "dataType": "Double"}, {"name": "CHECK", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "First Month", "dataType": "Date"}, {"name": "2023-07-01", "dataType": "Double"}, {"name": "2021-12-01", "dataType": "Double"}, {"name": "Start Date", "dataType": "String"}, {"name": "2022-01-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2022-08-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2022-10-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "Item", "dataType": "String"}, {"name": "Memo", "dataType": "String"}, {"name": "2024-12-01", "dataType": "Double"}, {"name": "2023-01-01", "dataType": "Double"}, {"name": "2021-04-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2021-11-01", "dataType": "Double"}, {"name": "2022-11-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Term2", "dataType": "String"}, {"name": "2024-09-01", "dataType": "Double"}, {"name": "2024-06-01", "dataType": "Double"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2023-03-01", "dataType": "Double"}, {"name": "2022-09-01", "dataType": "Double"}, {"name": "2022-07-01", "dataType": "Double"}, {"name": "2021-03-01", "dataType": "Double"}, {"name": "2024-11-01", "dataType": "Double"}, {"name": "Qty", "dataType": "Double"}, {"name": "Sales Price", "dataType": "String"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "2021-02-01", "dataType": "Double"}, {"name": "2022-05-01", "dataType": "Double"}, {"name": "2024-02-01", "dataType": "Double"}, {"name": "2021-10-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_3200_0 AS (

  SELECT 
    CAST(CASE
      WHEN CAST(isnull(YEAR (`Start Date`)) AS BOOLEAN)
        THEN 1
      ELSE 0
    END AS BOOLEAN) AS `Date Check`,
    * EXCEPT (`date check`)
  
  FROM Union_3198 AS in0

),

Filter_3201_reject_to_Filter_3202 AS (

  SELECT * 
  
  FROM Formula_3200_0 AS in0
  
  WHERE (
          ((NOT(NOT CAST(`Date Check` AS BOOLEAN))) OR ((NOT CAST(`Date Check` AS BOOLEAN)) IS NULL))
          AND (NOT((`Start Date` IS NULL) OR ((LENGTH(`Start Date`)) = 0)))
        )

),

DateTime_3203_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`Start Date`, 'MM/dd/yyyy')), 'yyyy-MM-dd')) AS `Start Date Clean`,
    *
  
  FROM Filter_3201_reject_to_Filter_3202 AS in0

),

Filter_3207 AS (

  SELECT * 
  
  FROM DateTime_3203_0 AS in0
  
  WHERE (NOT((`Start Date Clean` IS NULL) OR ((LENGTH(`Start Date Clean`)) = 0)))

),

Union_3211_reformat_1 AS (

  SELECT 
    `2021-02-01` AS `2021-02-01`,
    `2021-03-01` AS `2021-03-01`,
    `2021-04-01` AS `2021-04-01`,
    `2021-05-01` AS `2021-05-01`,
    `2021-06-01` AS `2021-06-01`,
    `2021-07-01` AS `2021-07-01`,
    `2021-08-01` AS `2021-08-01`,
    `2021-09-01` AS `2021-09-01`,
    `2021-10-01` AS `2021-10-01`,
    `2021-11-01` AS `2021-11-01`,
    `2021-12-01` AS `2021-12-01`,
    `2022-01-01` AS `2022-01-01`,
    `2022-02-01` AS `2022-02-01`,
    `2022-03-01` AS `2022-03-01`,
    `2022-04-01` AS `2022-04-01`,
    `2022-05-01` AS `2022-05-01`,
    `2022-06-01` AS `2022-06-01`,
    `2022-07-01` AS `2022-07-01`,
    `2022-08-01` AS `2022-08-01`,
    `2022-09-01` AS `2022-09-01`,
    `2022-10-01` AS `2022-10-01`,
    `2022-11-01` AS `2022-11-01`,
    `2022-12-01` AS `2022-12-01`,
    `2023-01-01` AS `2023-01-01`,
    `2023-02-01` AS `2023-02-01`,
    `2023-03-01` AS `2023-03-01`,
    `2023-04-01` AS `2023-04-01`,
    `2023-05-01` AS `2023-05-01`,
    `2023-06-01` AS `2023-06-01`,
    `2023-07-01` AS `2023-07-01`,
    `2023-08-01` AS `2023-08-01`,
    `2023-09-01` AS `2023-09-01`,
    `2023-10-01` AS `2023-10-01`,
    `2023-11-01` AS `2023-11-01`,
    `2023-12-01` AS `2023-12-01`,
    `2024-01-01` AS `2024-01-01`,
    `2024-02-01` AS `2024-02-01`,
    `2024-03-01` AS `2024-03-01`,
    `2024-04-01` AS `2024-04-01`,
    `2024-05-01` AS `2024-05-01`,
    `2024-06-01` AS `2024-06-01`,
    `2024-07-01` AS `2024-07-01`,
    `2024-08-01` AS `2024-08-01`,
    `2024-09-01` AS `2024-09-01`,
    `2024-10-01` AS `2024-10-01`,
    `2024-11-01` AS `2024-11-01`,
    `2024-12-01` AS `2024-12-01`,
    `2025-01-01` AS `2025-01-01`,
    `2025-02-01` AS `2025-02-01`,
    `2025-03-01` AS `2025-03-01`,
    `2025-04-01` AS `2025-04-01`,
    `2025-05-01` AS `2025-05-01`,
    `2025-06-01` AS `2025-06-01`,
    `2025-07-01` AS `2025-07-01`,
    `2025-08-01` AS `2025-08-01`,
    `2025-09-01` AS `2025-09-01`,
    `2025-10-01` AS `2025-10-01`,
    `2025-11-01` AS `2025-11-01`,
    `2025-12-01` AS `2025-12-01`,
    `2026-01-01` AS `2026-01-01`,
    `2026-02-01` AS `2026-02-01`,
    `2026-03-01` AS `2026-03-01`,
    `2026-04-01` AS `2026-04-01`,
    `2026-05-01` AS `2026-05-01`,
    `2026-06-01` AS `2026-06-01`,
    `2026-07-01` AS `2026-07-01`,
    `2026-08-01` AS `2026-08-01`,
    `2026-09-01` AS `2026-09-01`,
    `2026-10-01` AS `2026-10-01`,
    `2026-11-01` AS `2026-11-01`,
    `2026-12-01` AS `2026-12-01`,
    Amount AS Amount,
    CHECK AS `CHECK`,
    variableDate AS variableDate,
    `Date Check` AS `Date Check`,
    `End Date` AS `End Date`,
    F10 AS F10,
    F91 AS F91,
    `First Month` AS `First Month`,
    `First Month Revenue` AS `First Month Revenue`,
    CAST(`Invoice Date` AS string) AS `Invoice Date`,
    Item AS Item,
    Item2 AS Item2,
    `Last Month` AS `Last Month`,
    `Last Month Revenue` AS `Last Month Revenue`,
    Memo AS Memo,
    `Monthly Revenue` AS `Monthly Revenue`,
    Name AS Name,
    Num AS Num,
    Qty AS Qty,
    RecordID AS RecordID,
    `Sales Price` AS `Sales Price`,
    `Start Date` AS `Start Date`,
    CAST(`Start Date Clean` AS string) AS `Start Date Clean`,
    Term AS Term,
    Term2 AS Term2,
    variableType AS variableType
  
  FROM Filter_3207 AS in0

),

Filter_3201 AS (

  SELECT * 
  
  FROM Formula_3200_0 AS in0
  
  WHERE (NOT CAST(`Date Check` AS BOOLEAN))

),

Formula_3213_0 AS (

  SELECT 
    (TO_DATE(`Start Date`, 'yyyy-MM-dd')) AS `Start Date Clean`,
    *
  
  FROM Filter_3201 AS in0

),

Union_3211_reformat_2 AS (

  SELECT 
    `2021-02-01` AS `2021-02-01`,
    `2021-03-01` AS `2021-03-01`,
    `2021-04-01` AS `2021-04-01`,
    `2021-05-01` AS `2021-05-01`,
    `2021-06-01` AS `2021-06-01`,
    `2021-07-01` AS `2021-07-01`,
    `2021-08-01` AS `2021-08-01`,
    `2021-09-01` AS `2021-09-01`,
    `2021-10-01` AS `2021-10-01`,
    `2021-11-01` AS `2021-11-01`,
    `2021-12-01` AS `2021-12-01`,
    `2022-01-01` AS `2022-01-01`,
    `2022-02-01` AS `2022-02-01`,
    `2022-03-01` AS `2022-03-01`,
    `2022-04-01` AS `2022-04-01`,
    `2022-05-01` AS `2022-05-01`,
    `2022-06-01` AS `2022-06-01`,
    `2022-07-01` AS `2022-07-01`,
    `2022-08-01` AS `2022-08-01`,
    `2022-09-01` AS `2022-09-01`,
    `2022-10-01` AS `2022-10-01`,
    `2022-11-01` AS `2022-11-01`,
    `2022-12-01` AS `2022-12-01`,
    `2023-01-01` AS `2023-01-01`,
    `2023-02-01` AS `2023-02-01`,
    `2023-03-01` AS `2023-03-01`,
    `2023-04-01` AS `2023-04-01`,
    `2023-05-01` AS `2023-05-01`,
    `2023-06-01` AS `2023-06-01`,
    `2023-07-01` AS `2023-07-01`,
    `2023-08-01` AS `2023-08-01`,
    `2023-09-01` AS `2023-09-01`,
    `2023-10-01` AS `2023-10-01`,
    `2023-11-01` AS `2023-11-01`,
    `2023-12-01` AS `2023-12-01`,
    `2024-01-01` AS `2024-01-01`,
    `2024-02-01` AS `2024-02-01`,
    `2024-03-01` AS `2024-03-01`,
    `2024-04-01` AS `2024-04-01`,
    `2024-05-01` AS `2024-05-01`,
    `2024-06-01` AS `2024-06-01`,
    `2024-07-01` AS `2024-07-01`,
    `2024-08-01` AS `2024-08-01`,
    `2024-09-01` AS `2024-09-01`,
    `2024-10-01` AS `2024-10-01`,
    `2024-11-01` AS `2024-11-01`,
    `2024-12-01` AS `2024-12-01`,
    `2025-01-01` AS `2025-01-01`,
    `2025-02-01` AS `2025-02-01`,
    `2025-03-01` AS `2025-03-01`,
    `2025-04-01` AS `2025-04-01`,
    `2025-05-01` AS `2025-05-01`,
    `2025-06-01` AS `2025-06-01`,
    `2025-07-01` AS `2025-07-01`,
    `2025-08-01` AS `2025-08-01`,
    `2025-09-01` AS `2025-09-01`,
    `2025-10-01` AS `2025-10-01`,
    `2025-11-01` AS `2025-11-01`,
    `2025-12-01` AS `2025-12-01`,
    `2026-01-01` AS `2026-01-01`,
    `2026-02-01` AS `2026-02-01`,
    `2026-03-01` AS `2026-03-01`,
    `2026-04-01` AS `2026-04-01`,
    `2026-05-01` AS `2026-05-01`,
    `2026-06-01` AS `2026-06-01`,
    `2026-07-01` AS `2026-07-01`,
    `2026-08-01` AS `2026-08-01`,
    `2026-09-01` AS `2026-09-01`,
    `2026-10-01` AS `2026-10-01`,
    `2026-11-01` AS `2026-11-01`,
    `2026-12-01` AS `2026-12-01`,
    Amount AS Amount,
    CHECK AS `CHECK`,
    variableDate AS variableDate,
    `Date Check` AS `Date Check`,
    `End Date` AS `End Date`,
    F10 AS F10,
    F91 AS F91,
    `First Month` AS `First Month`,
    `First Month Revenue` AS `First Month Revenue`,
    CAST(`Invoice Date` AS string) AS `Invoice Date`,
    Item AS Item,
    Item2 AS Item2,
    `Last Month` AS `Last Month`,
    `Last Month Revenue` AS `Last Month Revenue`,
    Memo AS Memo,
    `Monthly Revenue` AS `Monthly Revenue`,
    Name AS Name,
    Num AS Num,
    Qty AS Qty,
    RecordID AS RecordID,
    `Sales Price` AS `Sales Price`,
    `Start Date` AS `Start Date`,
    CAST(`Start Date Clean` AS string) AS `Start Date Clean`,
    Term AS Term,
    Term2 AS Term2,
    variableType AS variableType
  
  FROM Formula_3213_0 AS in0

),

Filter_3207_reject AS (

  SELECT * 
  
  FROM DateTime_3203_0 AS in0
  
  WHERE (
          ((`Start Date Clean` IS NULL) OR ((LENGTH(`Start Date Clean`)) = 0))
          OR ((NOT((`Start Date Clean` IS NULL) OR ((LENGTH(`Start Date Clean`)) = 0))) IS NULL)
        )

),

AlteryxSelect_3210 AS (

  SELECT * EXCEPT (`Invoice Date`, `Start Date Clean`)
  
  FROM Filter_3207_reject AS in0

),

DateTime_3208_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`Start Date`, 'dd-MMM-yyyy')), 'yyyy-MM-dd')) AS `Start Date Clean`,
    *
  
  FROM AlteryxSelect_3210 AS in0

),

Filter_3209 AS (

  SELECT * 
  
  FROM DateTime_3208_0 AS in0
  
  WHERE (NOT((`Start Date Clean` IS NULL) OR ((LENGTH(`Start Date Clean`)) = 0)))

),

Union_3211_reformat_0 AS (

  SELECT 
    `2021-02-01` AS `2021-02-01`,
    `2021-03-01` AS `2021-03-01`,
    `2021-04-01` AS `2021-04-01`,
    `2021-05-01` AS `2021-05-01`,
    `2021-06-01` AS `2021-06-01`,
    `2021-07-01` AS `2021-07-01`,
    `2021-08-01` AS `2021-08-01`,
    `2021-09-01` AS `2021-09-01`,
    `2021-10-01` AS `2021-10-01`,
    `2021-11-01` AS `2021-11-01`,
    `2021-12-01` AS `2021-12-01`,
    `2022-01-01` AS `2022-01-01`,
    `2022-02-01` AS `2022-02-01`,
    `2022-03-01` AS `2022-03-01`,
    `2022-04-01` AS `2022-04-01`,
    `2022-05-01` AS `2022-05-01`,
    `2022-06-01` AS `2022-06-01`,
    `2022-07-01` AS `2022-07-01`,
    `2022-08-01` AS `2022-08-01`,
    `2022-09-01` AS `2022-09-01`,
    `2022-10-01` AS `2022-10-01`,
    `2022-11-01` AS `2022-11-01`,
    `2022-12-01` AS `2022-12-01`,
    `2023-01-01` AS `2023-01-01`,
    `2023-02-01` AS `2023-02-01`,
    `2023-03-01` AS `2023-03-01`,
    `2023-04-01` AS `2023-04-01`,
    `2023-05-01` AS `2023-05-01`,
    `2023-06-01` AS `2023-06-01`,
    `2023-07-01` AS `2023-07-01`,
    `2023-08-01` AS `2023-08-01`,
    `2023-09-01` AS `2023-09-01`,
    `2023-10-01` AS `2023-10-01`,
    `2023-11-01` AS `2023-11-01`,
    `2023-12-01` AS `2023-12-01`,
    `2024-01-01` AS `2024-01-01`,
    `2024-02-01` AS `2024-02-01`,
    `2024-03-01` AS `2024-03-01`,
    `2024-04-01` AS `2024-04-01`,
    `2024-05-01` AS `2024-05-01`,
    `2024-06-01` AS `2024-06-01`,
    `2024-07-01` AS `2024-07-01`,
    `2024-08-01` AS `2024-08-01`,
    `2024-09-01` AS `2024-09-01`,
    `2024-10-01` AS `2024-10-01`,
    `2024-11-01` AS `2024-11-01`,
    `2024-12-01` AS `2024-12-01`,
    `2025-01-01` AS `2025-01-01`,
    `2025-02-01` AS `2025-02-01`,
    `2025-03-01` AS `2025-03-01`,
    `2025-04-01` AS `2025-04-01`,
    `2025-05-01` AS `2025-05-01`,
    `2025-06-01` AS `2025-06-01`,
    `2025-07-01` AS `2025-07-01`,
    `2025-08-01` AS `2025-08-01`,
    `2025-09-01` AS `2025-09-01`,
    `2025-10-01` AS `2025-10-01`,
    `2025-11-01` AS `2025-11-01`,
    `2025-12-01` AS `2025-12-01`,
    `2026-01-01` AS `2026-01-01`,
    `2026-02-01` AS `2026-02-01`,
    `2026-03-01` AS `2026-03-01`,
    `2026-04-01` AS `2026-04-01`,
    `2026-05-01` AS `2026-05-01`,
    `2026-06-01` AS `2026-06-01`,
    `2026-07-01` AS `2026-07-01`,
    `2026-08-01` AS `2026-08-01`,
    `2026-09-01` AS `2026-09-01`,
    `2026-10-01` AS `2026-10-01`,
    `2026-11-01` AS `2026-11-01`,
    `2026-12-01` AS `2026-12-01`,
    Amount AS Amount,
    CHECK AS `CHECK`,
    variableDate AS variableDate,
    `Date Check` AS `Date Check`,
    `End Date` AS `End Date`,
    F10 AS F10,
    F91 AS F91,
    `First Month` AS `First Month`,
    `First Month Revenue` AS `First Month Revenue`,
    Item AS Item,
    Item2 AS Item2,
    `Last Month` AS `Last Month`,
    `Last Month Revenue` AS `Last Month Revenue`,
    Memo AS Memo,
    `Monthly Revenue` AS `Monthly Revenue`,
    Name AS Name,
    Num AS Num,
    Qty AS Qty,
    RecordID AS RecordID,
    `Sales Price` AS `Sales Price`,
    `Start Date` AS `Start Date`,
    CAST(`Start Date Clean` AS string) AS `Start Date Clean`,
    Term AS Term,
    Term2 AS Term2,
    variableType AS variableType
  
  FROM Filter_3209 AS in0

),

Union_3211 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_3211_reformat_1', 'Union_3211_reformat_0', 'Union_3211_reformat_2'], 
      [
        '[{"name": "2021-02-01", "dataType": "Double"}, {"name": "2021-03-01", "dataType": "Double"}, {"name": "2021-04-01", "dataType": "Double"}, {"name": "2021-05-01", "dataType": "Double"}, {"name": "2021-06-01", "dataType": "Double"}, {"name": "2021-07-01", "dataType": "Double"}, {"name": "2021-08-01", "dataType": "Double"}, {"name": "2021-09-01", "dataType": "Double"}, {"name": "2021-10-01", "dataType": "Double"}, {"name": "2021-11-01", "dataType": "Double"}, {"name": "2021-12-01", "dataType": "Double"}, {"name": "2022-01-01", "dataType": "Double"}, {"name": "2022-02-01", "dataType": "Double"}, {"name": "2022-03-01", "dataType": "Double"}, {"name": "2022-04-01", "dataType": "Double"}, {"name": "2022-05-01", "dataType": "Double"}, {"name": "2022-06-01", "dataType": "Double"}, {"name": "2022-07-01", "dataType": "Double"}, {"name": "2022-08-01", "dataType": "Double"}, {"name": "2022-09-01", "dataType": "Double"}, {"name": "2022-10-01", "dataType": "Double"}, {"name": "2022-11-01", "dataType": "Double"}, {"name": "2022-12-01", "dataType": "Double"}, {"name": "2023-01-01", "dataType": "Double"}, {"name": "2023-02-01", "dataType": "Double"}, {"name": "2023-03-01", "dataType": "Double"}, {"name": "2023-04-01", "dataType": "Double"}, {"name": "2023-05-01", "dataType": "Double"}, {"name": "2023-06-01", "dataType": "Double"}, {"name": "2023-07-01", "dataType": "Double"}, {"name": "2023-08-01", "dataType": "Double"}, {"name": "2023-09-01", "dataType": "Double"}, {"name": "2023-10-01", "dataType": "Double"}, {"name": "2023-11-01", "dataType": "Double"}, {"name": "2023-12-01", "dataType": "Double"}, {"name": "2024-01-01", "dataType": "Double"}, {"name": "2024-02-01", "dataType": "Double"}, {"name": "2024-03-01", "dataType": "Double"}, {"name": "2024-04-01", "dataType": "Double"}, {"name": "2024-05-01", "dataType": "Double"}, {"name": "2024-06-01", "dataType": "Double"}, {"name": "2024-07-01", "dataType": "Double"}, {"name": "2024-08-01", "dataType": "Double"}, {"name": "2024-09-01", "dataType": "Double"}, {"name": "2024-10-01", "dataType": "Double"}, {"name": "2024-11-01", "dataType": "Double"}, {"name": "2024-12-01", "dataType": "Double"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "Amount", "dataType": "Double"}, {"name": "CHECK", "dataType": "Double"}, {"name": "variableDate", "dataType": "String"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "End Date", "dataType": "String"}, {"name": "F10", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}, {"name": "First Month", "dataType": "Date"}, {"name": "First Month Revenue", "dataType": "Double"}, {"name": "Invoice Date", "dataType": "String"}, {"name": "Item", "dataType": "String"}, {"name": "Item2", "dataType": "String"}, {"name": "Last Month", "dataType": "Date"}, {"name": "Last Month Revenue", "dataType": "Double"}, {"name": "Memo", "dataType": "String"}, {"name": "Monthly Revenue", "dataType": "Double"}, {"name": "Name", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "Qty", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Sales Price", "dataType": "String"}, {"name": "Start Date", "dataType": "String"}, {"name": "Start Date Clean", "dataType": "String"}, {"name": "Term", "dataType": "String"}, {"name": "Term2", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
        '[{"name": "2021-02-01", "dataType": "Double"}, {"name": "2021-03-01", "dataType": "Double"}, {"name": "2021-04-01", "dataType": "Double"}, {"name": "2021-05-01", "dataType": "Double"}, {"name": "2021-06-01", "dataType": "Double"}, {"name": "2021-07-01", "dataType": "Double"}, {"name": "2021-08-01", "dataType": "Double"}, {"name": "2021-09-01", "dataType": "Double"}, {"name": "2021-10-01", "dataType": "Double"}, {"name": "2021-11-01", "dataType": "Double"}, {"name": "2021-12-01", "dataType": "Double"}, {"name": "2022-01-01", "dataType": "Double"}, {"name": "2022-02-01", "dataType": "Double"}, {"name": "2022-03-01", "dataType": "Double"}, {"name": "2022-04-01", "dataType": "Double"}, {"name": "2022-05-01", "dataType": "Double"}, {"name": "2022-06-01", "dataType": "Double"}, {"name": "2022-07-01", "dataType": "Double"}, {"name": "2022-08-01", "dataType": "Double"}, {"name": "2022-09-01", "dataType": "Double"}, {"name": "2022-10-01", "dataType": "Double"}, {"name": "2022-11-01", "dataType": "Double"}, {"name": "2022-12-01", "dataType": "Double"}, {"name": "2023-01-01", "dataType": "Double"}, {"name": "2023-02-01", "dataType": "Double"}, {"name": "2023-03-01", "dataType": "Double"}, {"name": "2023-04-01", "dataType": "Double"}, {"name": "2023-05-01", "dataType": "Double"}, {"name": "2023-06-01", "dataType": "Double"}, {"name": "2023-07-01", "dataType": "Double"}, {"name": "2023-08-01", "dataType": "Double"}, {"name": "2023-09-01", "dataType": "Double"}, {"name": "2023-10-01", "dataType": "Double"}, {"name": "2023-11-01", "dataType": "Double"}, {"name": "2023-12-01", "dataType": "Double"}, {"name": "2024-01-01", "dataType": "Double"}, {"name": "2024-02-01", "dataType": "Double"}, {"name": "2024-03-01", "dataType": "Double"}, {"name": "2024-04-01", "dataType": "Double"}, {"name": "2024-05-01", "dataType": "Double"}, {"name": "2024-06-01", "dataType": "Double"}, {"name": "2024-07-01", "dataType": "Double"}, {"name": "2024-08-01", "dataType": "Double"}, {"name": "2024-09-01", "dataType": "Double"}, {"name": "2024-10-01", "dataType": "Double"}, {"name": "2024-11-01", "dataType": "Double"}, {"name": "2024-12-01", "dataType": "Double"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "Amount", "dataType": "Double"}, {"name": "CHECK", "dataType": "Double"}, {"name": "variableDate", "dataType": "String"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "End Date", "dataType": "String"}, {"name": "F10", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}, {"name": "First Month", "dataType": "Date"}, {"name": "First Month Revenue", "dataType": "Double"}, {"name": "Item", "dataType": "String"}, {"name": "Item2", "dataType": "String"}, {"name": "Last Month", "dataType": "Date"}, {"name": "Last Month Revenue", "dataType": "Double"}, {"name": "Memo", "dataType": "String"}, {"name": "Monthly Revenue", "dataType": "Double"}, {"name": "Name", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "Qty", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Sales Price", "dataType": "String"}, {"name": "Start Date", "dataType": "String"}, {"name": "Start Date Clean", "dataType": "String"}, {"name": "Term", "dataType": "String"}, {"name": "Term2", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
        '[{"name": "2021-02-01", "dataType": "Double"}, {"name": "2021-03-01", "dataType": "Double"}, {"name": "2021-04-01", "dataType": "Double"}, {"name": "2021-05-01", "dataType": "Double"}, {"name": "2021-06-01", "dataType": "Double"}, {"name": "2021-07-01", "dataType": "Double"}, {"name": "2021-08-01", "dataType": "Double"}, {"name": "2021-09-01", "dataType": "Double"}, {"name": "2021-10-01", "dataType": "Double"}, {"name": "2021-11-01", "dataType": "Double"}, {"name": "2021-12-01", "dataType": "Double"}, {"name": "2022-01-01", "dataType": "Double"}, {"name": "2022-02-01", "dataType": "Double"}, {"name": "2022-03-01", "dataType": "Double"}, {"name": "2022-04-01", "dataType": "Double"}, {"name": "2022-05-01", "dataType": "Double"}, {"name": "2022-06-01", "dataType": "Double"}, {"name": "2022-07-01", "dataType": "Double"}, {"name": "2022-08-01", "dataType": "Double"}, {"name": "2022-09-01", "dataType": "Double"}, {"name": "2022-10-01", "dataType": "Double"}, {"name": "2022-11-01", "dataType": "Double"}, {"name": "2022-12-01", "dataType": "Double"}, {"name": "2023-01-01", "dataType": "Double"}, {"name": "2023-02-01", "dataType": "Double"}, {"name": "2023-03-01", "dataType": "Double"}, {"name": "2023-04-01", "dataType": "Double"}, {"name": "2023-05-01", "dataType": "Double"}, {"name": "2023-06-01", "dataType": "Double"}, {"name": "2023-07-01", "dataType": "Double"}, {"name": "2023-08-01", "dataType": "Double"}, {"name": "2023-09-01", "dataType": "Double"}, {"name": "2023-10-01", "dataType": "Double"}, {"name": "2023-11-01", "dataType": "Double"}, {"name": "2023-12-01", "dataType": "Double"}, {"name": "2024-01-01", "dataType": "Double"}, {"name": "2024-02-01", "dataType": "Double"}, {"name": "2024-03-01", "dataType": "Double"}, {"name": "2024-04-01", "dataType": "Double"}, {"name": "2024-05-01", "dataType": "Double"}, {"name": "2024-06-01", "dataType": "Double"}, {"name": "2024-07-01", "dataType": "Double"}, {"name": "2024-08-01", "dataType": "Double"}, {"name": "2024-09-01", "dataType": "Double"}, {"name": "2024-10-01", "dataType": "Double"}, {"name": "2024-11-01", "dataType": "Double"}, {"name": "2024-12-01", "dataType": "Double"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "Amount", "dataType": "Double"}, {"name": "CHECK", "dataType": "Double"}, {"name": "variableDate", "dataType": "String"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "End Date", "dataType": "String"}, {"name": "F10", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}, {"name": "First Month", "dataType": "Date"}, {"name": "First Month Revenue", "dataType": "Double"}, {"name": "Invoice Date", "dataType": "String"}, {"name": "Item", "dataType": "String"}, {"name": "Item2", "dataType": "String"}, {"name": "Last Month", "dataType": "Date"}, {"name": "Last Month Revenue", "dataType": "Double"}, {"name": "Memo", "dataType": "String"}, {"name": "Monthly Revenue", "dataType": "Double"}, {"name": "Name", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "Qty", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Sales Price", "dataType": "String"}, {"name": "Start Date", "dataType": "String"}, {"name": "Start Date Clean", "dataType": "String"}, {"name": "Term", "dataType": "String"}, {"name": "Term2", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_3214_0 AS (

  SELECT 
    CAST(CASE
      WHEN CAST(isnull(YEAR (`End Date`)) AS BOOLEAN)
        THEN 1
      ELSE 0
    END AS BOOLEAN) AS `Date Check`,
    * EXCEPT (`date check`)
  
  FROM Union_3211 AS in0

),

Filter_3215_reject_to_Filter_3216 AS (

  SELECT * 
  
  FROM Formula_3214_0 AS in0
  
  WHERE (
          ((NOT(NOT CAST(`Date Check` AS BOOLEAN))) OR ((NOT CAST(`Date Check` AS BOOLEAN)) IS NULL))
          AND (NOT((`End Date` IS NULL) OR ((LENGTH(`End Date`)) = 0)))
        )

),

DateTime_3217_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`End Date`, 'MM/dd/yyyy')), 'yyyy-MM-dd')) AS `End Date Clean`,
    *
  
  FROM Filter_3215_reject_to_Filter_3216 AS in0

),

Filter_3221 AS (

  SELECT * 
  
  FROM DateTime_3217_0 AS in0
  
  WHERE (NOT((`End Date Clean` IS NULL) OR ((LENGTH(`End Date Clean`)) = 0)))

),

Union_3225_reformat_1 AS (

  SELECT 
    `2021-02-01` AS `2021-02-01`,
    `2021-03-01` AS `2021-03-01`,
    `2021-04-01` AS `2021-04-01`,
    `2021-05-01` AS `2021-05-01`,
    `2021-06-01` AS `2021-06-01`,
    `2021-07-01` AS `2021-07-01`,
    `2021-08-01` AS `2021-08-01`,
    `2021-09-01` AS `2021-09-01`,
    `2021-10-01` AS `2021-10-01`,
    `2021-11-01` AS `2021-11-01`,
    `2021-12-01` AS `2021-12-01`,
    `2022-01-01` AS `2022-01-01`,
    `2022-02-01` AS `2022-02-01`,
    `2022-03-01` AS `2022-03-01`,
    `2022-04-01` AS `2022-04-01`,
    `2022-05-01` AS `2022-05-01`,
    `2022-06-01` AS `2022-06-01`,
    `2022-07-01` AS `2022-07-01`,
    `2022-08-01` AS `2022-08-01`,
    `2022-09-01` AS `2022-09-01`,
    `2022-10-01` AS `2022-10-01`,
    `2022-11-01` AS `2022-11-01`,
    `2022-12-01` AS `2022-12-01`,
    `2023-01-01` AS `2023-01-01`,
    `2023-02-01` AS `2023-02-01`,
    `2023-03-01` AS `2023-03-01`,
    `2023-04-01` AS `2023-04-01`,
    `2023-05-01` AS `2023-05-01`,
    `2023-06-01` AS `2023-06-01`,
    `2023-07-01` AS `2023-07-01`,
    `2023-08-01` AS `2023-08-01`,
    `2023-09-01` AS `2023-09-01`,
    `2023-10-01` AS `2023-10-01`,
    `2023-11-01` AS `2023-11-01`,
    `2023-12-01` AS `2023-12-01`,
    `2024-01-01` AS `2024-01-01`,
    `2024-02-01` AS `2024-02-01`,
    `2024-03-01` AS `2024-03-01`,
    `2024-04-01` AS `2024-04-01`,
    `2024-05-01` AS `2024-05-01`,
    `2024-06-01` AS `2024-06-01`,
    `2024-07-01` AS `2024-07-01`,
    `2024-08-01` AS `2024-08-01`,
    `2024-09-01` AS `2024-09-01`,
    `2024-10-01` AS `2024-10-01`,
    `2024-11-01` AS `2024-11-01`,
    `2024-12-01` AS `2024-12-01`,
    `2025-01-01` AS `2025-01-01`,
    `2025-02-01` AS `2025-02-01`,
    `2025-03-01` AS `2025-03-01`,
    `2025-04-01` AS `2025-04-01`,
    `2025-05-01` AS `2025-05-01`,
    `2025-06-01` AS `2025-06-01`,
    `2025-07-01` AS `2025-07-01`,
    `2025-08-01` AS `2025-08-01`,
    `2025-09-01` AS `2025-09-01`,
    `2025-10-01` AS `2025-10-01`,
    `2025-11-01` AS `2025-11-01`,
    `2025-12-01` AS `2025-12-01`,
    `2026-01-01` AS `2026-01-01`,
    `2026-02-01` AS `2026-02-01`,
    `2026-03-01` AS `2026-03-01`,
    `2026-04-01` AS `2026-04-01`,
    `2026-05-01` AS `2026-05-01`,
    `2026-06-01` AS `2026-06-01`,
    `2026-07-01` AS `2026-07-01`,
    `2026-08-01` AS `2026-08-01`,
    `2026-09-01` AS `2026-09-01`,
    `2026-10-01` AS `2026-10-01`,
    `2026-11-01` AS `2026-11-01`,
    `2026-12-01` AS `2026-12-01`,
    Amount AS Amount,
    CHECK AS `CHECK`,
    variableDate AS variableDate,
    `Date Check` AS `Date Check`,
    `End Date` AS `End Date`,
    CAST(`End Date Clean` AS string) AS `End Date Clean`,
    F10 AS F10,
    F91 AS F91,
    `First Month` AS `First Month`,
    `First Month Revenue` AS `First Month Revenue`,
    CAST(`Invoice Date` AS string) AS `Invoice Date`,
    Item AS Item,
    Item2 AS Item2,
    `Last Month` AS `Last Month`,
    `Last Month Revenue` AS `Last Month Revenue`,
    Memo AS Memo,
    `Monthly Revenue` AS `Monthly Revenue`,
    Name AS Name,
    Num AS Num,
    Qty AS Qty,
    RecordID AS RecordID,
    `Sales Price` AS `Sales Price`,
    `Start Date` AS `Start Date`,
    CAST(`Start Date Clean` AS string) AS `Start Date Clean`,
    Term AS Term,
    Term2 AS Term2,
    variableType AS variableType
  
  FROM Filter_3221 AS in0

),

Filter_3221_reject AS (

  SELECT * 
  
  FROM DateTime_3217_0 AS in0
  
  WHERE (
          ((`End Date Clean` IS NULL) OR ((LENGTH(`End Date Clean`)) = 0))
          OR ((NOT((`End Date Clean` IS NULL) OR ((LENGTH(`End Date Clean`)) = 0))) IS NULL)
        )

),

AlteryxSelect_3224 AS (

  SELECT * EXCEPT (`Invoice Date`, `Start Date Clean`, `End Date Clean`)
  
  FROM Filter_3221_reject AS in0

),

DateTime_3222_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(`End Date`, 'dd-MMM-yyyy')), 'yyyy-MM-dd')) AS `End Date Clean`,
    *
  
  FROM AlteryxSelect_3224 AS in0

),

Filter_3223 AS (

  SELECT * 
  
  FROM DateTime_3222_0 AS in0
  
  WHERE (NOT((`End Date Clean` IS NULL) OR ((LENGTH(`End Date Clean`)) = 0)))

),

Union_3225_reformat_0 AS (

  SELECT 
    `2021-02-01` AS `2021-02-01`,
    `2021-03-01` AS `2021-03-01`,
    `2021-04-01` AS `2021-04-01`,
    `2021-05-01` AS `2021-05-01`,
    `2021-06-01` AS `2021-06-01`,
    `2021-07-01` AS `2021-07-01`,
    `2021-08-01` AS `2021-08-01`,
    `2021-09-01` AS `2021-09-01`,
    `2021-10-01` AS `2021-10-01`,
    `2021-11-01` AS `2021-11-01`,
    `2021-12-01` AS `2021-12-01`,
    `2022-01-01` AS `2022-01-01`,
    `2022-02-01` AS `2022-02-01`,
    `2022-03-01` AS `2022-03-01`,
    `2022-04-01` AS `2022-04-01`,
    `2022-05-01` AS `2022-05-01`,
    `2022-06-01` AS `2022-06-01`,
    `2022-07-01` AS `2022-07-01`,
    `2022-08-01` AS `2022-08-01`,
    `2022-09-01` AS `2022-09-01`,
    `2022-10-01` AS `2022-10-01`,
    `2022-11-01` AS `2022-11-01`,
    `2022-12-01` AS `2022-12-01`,
    `2023-01-01` AS `2023-01-01`,
    `2023-02-01` AS `2023-02-01`,
    `2023-03-01` AS `2023-03-01`,
    `2023-04-01` AS `2023-04-01`,
    `2023-05-01` AS `2023-05-01`,
    `2023-06-01` AS `2023-06-01`,
    `2023-07-01` AS `2023-07-01`,
    `2023-08-01` AS `2023-08-01`,
    `2023-09-01` AS `2023-09-01`,
    `2023-10-01` AS `2023-10-01`,
    `2023-11-01` AS `2023-11-01`,
    `2023-12-01` AS `2023-12-01`,
    `2024-01-01` AS `2024-01-01`,
    `2024-02-01` AS `2024-02-01`,
    `2024-03-01` AS `2024-03-01`,
    `2024-04-01` AS `2024-04-01`,
    `2024-05-01` AS `2024-05-01`,
    `2024-06-01` AS `2024-06-01`,
    `2024-07-01` AS `2024-07-01`,
    `2024-08-01` AS `2024-08-01`,
    `2024-09-01` AS `2024-09-01`,
    `2024-10-01` AS `2024-10-01`,
    `2024-11-01` AS `2024-11-01`,
    `2024-12-01` AS `2024-12-01`,
    `2025-01-01` AS `2025-01-01`,
    `2025-02-01` AS `2025-02-01`,
    `2025-03-01` AS `2025-03-01`,
    `2025-04-01` AS `2025-04-01`,
    `2025-05-01` AS `2025-05-01`,
    `2025-06-01` AS `2025-06-01`,
    `2025-07-01` AS `2025-07-01`,
    `2025-08-01` AS `2025-08-01`,
    `2025-09-01` AS `2025-09-01`,
    `2025-10-01` AS `2025-10-01`,
    `2025-11-01` AS `2025-11-01`,
    `2025-12-01` AS `2025-12-01`,
    `2026-01-01` AS `2026-01-01`,
    `2026-02-01` AS `2026-02-01`,
    `2026-03-01` AS `2026-03-01`,
    `2026-04-01` AS `2026-04-01`,
    `2026-05-01` AS `2026-05-01`,
    `2026-06-01` AS `2026-06-01`,
    `2026-07-01` AS `2026-07-01`,
    `2026-08-01` AS `2026-08-01`,
    `2026-09-01` AS `2026-09-01`,
    `2026-10-01` AS `2026-10-01`,
    `2026-11-01` AS `2026-11-01`,
    `2026-12-01` AS `2026-12-01`,
    Amount AS Amount,
    CHECK AS `CHECK`,
    variableDate AS variableDate,
    `Date Check` AS `Date Check`,
    `End Date` AS `End Date`,
    CAST(`End Date Clean` AS string) AS `End Date Clean`,
    F10 AS F10,
    F91 AS F91,
    `First Month` AS `First Month`,
    `First Month Revenue` AS `First Month Revenue`,
    Item AS Item,
    Item2 AS Item2,
    `Last Month` AS `Last Month`,
    `Last Month Revenue` AS `Last Month Revenue`,
    Memo AS Memo,
    `Monthly Revenue` AS `Monthly Revenue`,
    Name AS Name,
    Num AS Num,
    Qty AS Qty,
    RecordID AS RecordID,
    `Sales Price` AS `Sales Price`,
    `Start Date` AS `Start Date`,
    Term AS Term,
    Term2 AS Term2,
    variableType AS variableType
  
  FROM Filter_3223 AS in0

),

Filter_3215 AS (

  SELECT * 
  
  FROM Formula_3214_0 AS in0
  
  WHERE (NOT CAST(`Date Check` AS BOOLEAN))

),

Formula_3227_0 AS (

  SELECT 
    (TO_DATE(`End Date`, 'yyyy-MM-dd')) AS `End Date Clean`,
    *
  
  FROM Filter_3215 AS in0

),

Union_3225_reformat_2 AS (

  SELECT 
    `2021-02-01` AS `2021-02-01`,
    `2021-03-01` AS `2021-03-01`,
    `2021-04-01` AS `2021-04-01`,
    `2021-05-01` AS `2021-05-01`,
    `2021-06-01` AS `2021-06-01`,
    `2021-07-01` AS `2021-07-01`,
    `2021-08-01` AS `2021-08-01`,
    `2021-09-01` AS `2021-09-01`,
    `2021-10-01` AS `2021-10-01`,
    `2021-11-01` AS `2021-11-01`,
    `2021-12-01` AS `2021-12-01`,
    `2022-01-01` AS `2022-01-01`,
    `2022-02-01` AS `2022-02-01`,
    `2022-03-01` AS `2022-03-01`,
    `2022-04-01` AS `2022-04-01`,
    `2022-05-01` AS `2022-05-01`,
    `2022-06-01` AS `2022-06-01`,
    `2022-07-01` AS `2022-07-01`,
    `2022-08-01` AS `2022-08-01`,
    `2022-09-01` AS `2022-09-01`,
    `2022-10-01` AS `2022-10-01`,
    `2022-11-01` AS `2022-11-01`,
    `2022-12-01` AS `2022-12-01`,
    `2023-01-01` AS `2023-01-01`,
    `2023-02-01` AS `2023-02-01`,
    `2023-03-01` AS `2023-03-01`,
    `2023-04-01` AS `2023-04-01`,
    `2023-05-01` AS `2023-05-01`,
    `2023-06-01` AS `2023-06-01`,
    `2023-07-01` AS `2023-07-01`,
    `2023-08-01` AS `2023-08-01`,
    `2023-09-01` AS `2023-09-01`,
    `2023-10-01` AS `2023-10-01`,
    `2023-11-01` AS `2023-11-01`,
    `2023-12-01` AS `2023-12-01`,
    `2024-01-01` AS `2024-01-01`,
    `2024-02-01` AS `2024-02-01`,
    `2024-03-01` AS `2024-03-01`,
    `2024-04-01` AS `2024-04-01`,
    `2024-05-01` AS `2024-05-01`,
    `2024-06-01` AS `2024-06-01`,
    `2024-07-01` AS `2024-07-01`,
    `2024-08-01` AS `2024-08-01`,
    `2024-09-01` AS `2024-09-01`,
    `2024-10-01` AS `2024-10-01`,
    `2024-11-01` AS `2024-11-01`,
    `2024-12-01` AS `2024-12-01`,
    `2025-01-01` AS `2025-01-01`,
    `2025-02-01` AS `2025-02-01`,
    `2025-03-01` AS `2025-03-01`,
    `2025-04-01` AS `2025-04-01`,
    `2025-05-01` AS `2025-05-01`,
    `2025-06-01` AS `2025-06-01`,
    `2025-07-01` AS `2025-07-01`,
    `2025-08-01` AS `2025-08-01`,
    `2025-09-01` AS `2025-09-01`,
    `2025-10-01` AS `2025-10-01`,
    `2025-11-01` AS `2025-11-01`,
    `2025-12-01` AS `2025-12-01`,
    `2026-01-01` AS `2026-01-01`,
    `2026-02-01` AS `2026-02-01`,
    `2026-03-01` AS `2026-03-01`,
    `2026-04-01` AS `2026-04-01`,
    `2026-05-01` AS `2026-05-01`,
    `2026-06-01` AS `2026-06-01`,
    `2026-07-01` AS `2026-07-01`,
    `2026-08-01` AS `2026-08-01`,
    `2026-09-01` AS `2026-09-01`,
    `2026-10-01` AS `2026-10-01`,
    `2026-11-01` AS `2026-11-01`,
    `2026-12-01` AS `2026-12-01`,
    Amount AS Amount,
    CHECK AS `CHECK`,
    variableDate AS variableDate,
    `Date Check` AS `Date Check`,
    `End Date` AS `End Date`,
    CAST(`End Date Clean` AS string) AS `End Date Clean`,
    F10 AS F10,
    F91 AS F91,
    `First Month` AS `First Month`,
    `First Month Revenue` AS `First Month Revenue`,
    CAST(`Invoice Date` AS string) AS `Invoice Date`,
    Item AS Item,
    Item2 AS Item2,
    `Last Month` AS `Last Month`,
    `Last Month Revenue` AS `Last Month Revenue`,
    Memo AS Memo,
    `Monthly Revenue` AS `Monthly Revenue`,
    Name AS Name,
    Num AS Num,
    Qty AS Qty,
    RecordID AS RecordID,
    `Sales Price` AS `Sales Price`,
    `Start Date` AS `Start Date`,
    CAST(`Start Date Clean` AS string) AS `Start Date Clean`,
    Term AS Term,
    Term2 AS Term2,
    variableType AS variableType
  
  FROM Formula_3227_0 AS in0

),

Union_3225 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_3225_reformat_1', 'Union_3225_reformat_0', 'Union_3225_reformat_2'], 
      [
        '[{"name": "2021-02-01", "dataType": "Double"}, {"name": "2021-03-01", "dataType": "Double"}, {"name": "2021-04-01", "dataType": "Double"}, {"name": "2021-05-01", "dataType": "Double"}, {"name": "2021-06-01", "dataType": "Double"}, {"name": "2021-07-01", "dataType": "Double"}, {"name": "2021-08-01", "dataType": "Double"}, {"name": "2021-09-01", "dataType": "Double"}, {"name": "2021-10-01", "dataType": "Double"}, {"name": "2021-11-01", "dataType": "Double"}, {"name": "2021-12-01", "dataType": "Double"}, {"name": "2022-01-01", "dataType": "Double"}, {"name": "2022-02-01", "dataType": "Double"}, {"name": "2022-03-01", "dataType": "Double"}, {"name": "2022-04-01", "dataType": "Double"}, {"name": "2022-05-01", "dataType": "Double"}, {"name": "2022-06-01", "dataType": "Double"}, {"name": "2022-07-01", "dataType": "Double"}, {"name": "2022-08-01", "dataType": "Double"}, {"name": "2022-09-01", "dataType": "Double"}, {"name": "2022-10-01", "dataType": "Double"}, {"name": "2022-11-01", "dataType": "Double"}, {"name": "2022-12-01", "dataType": "Double"}, {"name": "2023-01-01", "dataType": "Double"}, {"name": "2023-02-01", "dataType": "Double"}, {"name": "2023-03-01", "dataType": "Double"}, {"name": "2023-04-01", "dataType": "Double"}, {"name": "2023-05-01", "dataType": "Double"}, {"name": "2023-06-01", "dataType": "Double"}, {"name": "2023-07-01", "dataType": "Double"}, {"name": "2023-08-01", "dataType": "Double"}, {"name": "2023-09-01", "dataType": "Double"}, {"name": "2023-10-01", "dataType": "Double"}, {"name": "2023-11-01", "dataType": "Double"}, {"name": "2023-12-01", "dataType": "Double"}, {"name": "2024-01-01", "dataType": "Double"}, {"name": "2024-02-01", "dataType": "Double"}, {"name": "2024-03-01", "dataType": "Double"}, {"name": "2024-04-01", "dataType": "Double"}, {"name": "2024-05-01", "dataType": "Double"}, {"name": "2024-06-01", "dataType": "Double"}, {"name": "2024-07-01", "dataType": "Double"}, {"name": "2024-08-01", "dataType": "Double"}, {"name": "2024-09-01", "dataType": "Double"}, {"name": "2024-10-01", "dataType": "Double"}, {"name": "2024-11-01", "dataType": "Double"}, {"name": "2024-12-01", "dataType": "Double"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "Amount", "dataType": "Double"}, {"name": "CHECK", "dataType": "Double"}, {"name": "variableDate", "dataType": "String"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "End Date", "dataType": "String"}, {"name": "End Date Clean", "dataType": "String"}, {"name": "F10", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}, {"name": "First Month", "dataType": "Date"}, {"name": "First Month Revenue", "dataType": "Double"}, {"name": "Invoice Date", "dataType": "String"}, {"name": "Item", "dataType": "String"}, {"name": "Item2", "dataType": "String"}, {"name": "Last Month", "dataType": "Date"}, {"name": "Last Month Revenue", "dataType": "Double"}, {"name": "Memo", "dataType": "String"}, {"name": "Monthly Revenue", "dataType": "Double"}, {"name": "Name", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "Qty", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Sales Price", "dataType": "String"}, {"name": "Start Date", "dataType": "String"}, {"name": "Start Date Clean", "dataType": "String"}, {"name": "Term", "dataType": "String"}, {"name": "Term2", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
        '[{"name": "2021-02-01", "dataType": "Double"}, {"name": "2021-03-01", "dataType": "Double"}, {"name": "2021-04-01", "dataType": "Double"}, {"name": "2021-05-01", "dataType": "Double"}, {"name": "2021-06-01", "dataType": "Double"}, {"name": "2021-07-01", "dataType": "Double"}, {"name": "2021-08-01", "dataType": "Double"}, {"name": "2021-09-01", "dataType": "Double"}, {"name": "2021-10-01", "dataType": "Double"}, {"name": "2021-11-01", "dataType": "Double"}, {"name": "2021-12-01", "dataType": "Double"}, {"name": "2022-01-01", "dataType": "Double"}, {"name": "2022-02-01", "dataType": "Double"}, {"name": "2022-03-01", "dataType": "Double"}, {"name": "2022-04-01", "dataType": "Double"}, {"name": "2022-05-01", "dataType": "Double"}, {"name": "2022-06-01", "dataType": "Double"}, {"name": "2022-07-01", "dataType": "Double"}, {"name": "2022-08-01", "dataType": "Double"}, {"name": "2022-09-01", "dataType": "Double"}, {"name": "2022-10-01", "dataType": "Double"}, {"name": "2022-11-01", "dataType": "Double"}, {"name": "2022-12-01", "dataType": "Double"}, {"name": "2023-01-01", "dataType": "Double"}, {"name": "2023-02-01", "dataType": "Double"}, {"name": "2023-03-01", "dataType": "Double"}, {"name": "2023-04-01", "dataType": "Double"}, {"name": "2023-05-01", "dataType": "Double"}, {"name": "2023-06-01", "dataType": "Double"}, {"name": "2023-07-01", "dataType": "Double"}, {"name": "2023-08-01", "dataType": "Double"}, {"name": "2023-09-01", "dataType": "Double"}, {"name": "2023-10-01", "dataType": "Double"}, {"name": "2023-11-01", "dataType": "Double"}, {"name": "2023-12-01", "dataType": "Double"}, {"name": "2024-01-01", "dataType": "Double"}, {"name": "2024-02-01", "dataType": "Double"}, {"name": "2024-03-01", "dataType": "Double"}, {"name": "2024-04-01", "dataType": "Double"}, {"name": "2024-05-01", "dataType": "Double"}, {"name": "2024-06-01", "dataType": "Double"}, {"name": "2024-07-01", "dataType": "Double"}, {"name": "2024-08-01", "dataType": "Double"}, {"name": "2024-09-01", "dataType": "Double"}, {"name": "2024-10-01", "dataType": "Double"}, {"name": "2024-11-01", "dataType": "Double"}, {"name": "2024-12-01", "dataType": "Double"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "Amount", "dataType": "Double"}, {"name": "CHECK", "dataType": "Double"}, {"name": "variableDate", "dataType": "String"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "End Date", "dataType": "String"}, {"name": "End Date Clean", "dataType": "String"}, {"name": "F10", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}, {"name": "First Month", "dataType": "Date"}, {"name": "First Month Revenue", "dataType": "Double"}, {"name": "Item", "dataType": "String"}, {"name": "Item2", "dataType": "String"}, {"name": "Last Month", "dataType": "Date"}, {"name": "Last Month Revenue", "dataType": "Double"}, {"name": "Memo", "dataType": "String"}, {"name": "Monthly Revenue", "dataType": "Double"}, {"name": "Name", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "Qty", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Sales Price", "dataType": "String"}, {"name": "Start Date", "dataType": "String"}, {"name": "Term", "dataType": "String"}, {"name": "Term2", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
        '[{"name": "2021-02-01", "dataType": "Double"}, {"name": "2021-03-01", "dataType": "Double"}, {"name": "2021-04-01", "dataType": "Double"}, {"name": "2021-05-01", "dataType": "Double"}, {"name": "2021-06-01", "dataType": "Double"}, {"name": "2021-07-01", "dataType": "Double"}, {"name": "2021-08-01", "dataType": "Double"}, {"name": "2021-09-01", "dataType": "Double"}, {"name": "2021-10-01", "dataType": "Double"}, {"name": "2021-11-01", "dataType": "Double"}, {"name": "2021-12-01", "dataType": "Double"}, {"name": "2022-01-01", "dataType": "Double"}, {"name": "2022-02-01", "dataType": "Double"}, {"name": "2022-03-01", "dataType": "Double"}, {"name": "2022-04-01", "dataType": "Double"}, {"name": "2022-05-01", "dataType": "Double"}, {"name": "2022-06-01", "dataType": "Double"}, {"name": "2022-07-01", "dataType": "Double"}, {"name": "2022-08-01", "dataType": "Double"}, {"name": "2022-09-01", "dataType": "Double"}, {"name": "2022-10-01", "dataType": "Double"}, {"name": "2022-11-01", "dataType": "Double"}, {"name": "2022-12-01", "dataType": "Double"}, {"name": "2023-01-01", "dataType": "Double"}, {"name": "2023-02-01", "dataType": "Double"}, {"name": "2023-03-01", "dataType": "Double"}, {"name": "2023-04-01", "dataType": "Double"}, {"name": "2023-05-01", "dataType": "Double"}, {"name": "2023-06-01", "dataType": "Double"}, {"name": "2023-07-01", "dataType": "Double"}, {"name": "2023-08-01", "dataType": "Double"}, {"name": "2023-09-01", "dataType": "Double"}, {"name": "2023-10-01", "dataType": "Double"}, {"name": "2023-11-01", "dataType": "Double"}, {"name": "2023-12-01", "dataType": "Double"}, {"name": "2024-01-01", "dataType": "Double"}, {"name": "2024-02-01", "dataType": "Double"}, {"name": "2024-03-01", "dataType": "Double"}, {"name": "2024-04-01", "dataType": "Double"}, {"name": "2024-05-01", "dataType": "Double"}, {"name": "2024-06-01", "dataType": "Double"}, {"name": "2024-07-01", "dataType": "Double"}, {"name": "2024-08-01", "dataType": "Double"}, {"name": "2024-09-01", "dataType": "Double"}, {"name": "2024-10-01", "dataType": "Double"}, {"name": "2024-11-01", "dataType": "Double"}, {"name": "2024-12-01", "dataType": "Double"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "Amount", "dataType": "Double"}, {"name": "CHECK", "dataType": "Double"}, {"name": "variableDate", "dataType": "String"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "End Date", "dataType": "String"}, {"name": "End Date Clean", "dataType": "String"}, {"name": "F10", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}, {"name": "First Month", "dataType": "Date"}, {"name": "First Month Revenue", "dataType": "Double"}, {"name": "Invoice Date", "dataType": "String"}, {"name": "Item", "dataType": "String"}, {"name": "Item2", "dataType": "String"}, {"name": "Last Month", "dataType": "Date"}, {"name": "Last Month Revenue", "dataType": "Double"}, {"name": "Memo", "dataType": "String"}, {"name": "Monthly Revenue", "dataType": "Double"}, {"name": "Name", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "Qty", "dataType": "Double"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Sales Price", "dataType": "String"}, {"name": "Start Date", "dataType": "String"}, {"name": "Start Date Clean", "dataType": "String"}, {"name": "Term", "dataType": "String"}, {"name": "Term2", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_3164 AS (

  SELECT 
    RecordID AS RecordID,
    variableType AS variableType,
    `Invoice Date` AS `Invoice Date`,
    `Start Date Clean` AS `Start Date`,
    `End Date Clean` AS `End Date`,
    Num AS Num,
    Memo AS Memo,
    Name AS Name,
    Item AS Item,
    Qty AS Qty,
    `Sales Price` AS `Sales Price`,
    Amount AS Amount,
    F10 AS F10,
    Term AS Term,
    `First Month` AS `First Month`,
    `Last Month` AS `Last Month`,
    `Monthly Revenue` AS `Monthly Revenue`,
    `First Month Revenue` AS `First Month Revenue`,
    `Last Month Revenue` AS `Last Month Revenue`,
    `2021-02-01` AS `2021-02-01`,
    `2021-03-01` AS `2021-03-01`,
    `2021-04-01` AS `2021-04-01`,
    `2021-05-01` AS `2021-05-01`,
    `2021-06-01` AS `2021-06-01`,
    `2021-07-01` AS `2021-07-01`,
    `2021-08-01` AS `2021-08-01`,
    `2021-09-01` AS `2021-09-01`,
    `2021-10-01` AS `2021-10-01`,
    `2021-11-01` AS `2021-11-01`,
    `2021-12-01` AS `2021-12-01`,
    `2022-01-01` AS `2022-01-01`,
    `2022-02-01` AS `2022-02-01`,
    `2022-03-01` AS `2022-03-01`,
    `2022-04-01` AS `2022-04-01`,
    `2022-05-01` AS `2022-05-01`,
    `2022-06-01` AS `2022-06-01`,
    `2022-07-01` AS `2022-07-01`,
    `2022-08-01` AS `2022-08-01`,
    `2022-09-01` AS `2022-09-01`,
    `2022-10-01` AS `2022-10-01`,
    `2022-11-01` AS `2022-11-01`,
    `2022-12-01` AS `2022-12-01`,
    `2023-01-01` AS `2023-01-01`,
    `2023-02-01` AS `2023-02-01`,
    `2023-03-01` AS `2023-03-01`,
    `2023-04-01` AS `2023-04-01`,
    `2023-05-01` AS `2023-05-01`,
    `2023-06-01` AS `2023-06-01`,
    `2023-07-01` AS `2023-07-01`,
    `2023-08-01` AS `2023-08-01`,
    `2023-09-01` AS `2023-09-01`,
    `2023-10-01` AS `2023-10-01`,
    `2023-11-01` AS `2023-11-01`,
    `2023-12-01` AS `2023-12-01`,
    `2024-01-01` AS `2024-01-01`,
    `2024-02-01` AS `2024-02-01`,
    `2024-03-01` AS `2024-03-01`,
    `2024-04-01` AS `2024-04-01`,
    `2024-05-01` AS `2024-05-01`,
    `2024-06-01` AS `2024-06-01`,
    `2024-07-01` AS `2024-07-01`,
    `2024-08-01` AS `2024-08-01`,
    `2024-09-01` AS `2024-09-01`,
    `2024-10-01` AS `2024-10-01`,
    `2024-11-01` AS `2024-11-01`,
    `2024-12-01` AS `2024-12-01`,
    `2025-01-01` AS `2025-01-01`,
    `2025-02-01` AS `2025-02-01`,
    `2025-03-01` AS `2025-03-01`,
    `2025-04-01` AS `2025-04-01`,
    `2025-05-01` AS `2025-05-01`,
    `2025-06-01` AS `2025-06-01`,
    `2025-07-01` AS `2025-07-01`,
    `2025-08-01` AS `2025-08-01`,
    `2025-09-01` AS `2025-09-01`,
    `2025-10-01` AS `2025-10-01`,
    `2025-11-01` AS `2025-11-01`,
    `2025-12-01` AS `2025-12-01`,
    CHECK AS `CHECK`,
    Item2 AS Item2,
    Term2 AS Term2,
    `Date Check` AS `Date Check`,
    variableDate AS variableDate,
    * EXCEPT (`Start Date`, 
    `End Date`, 
    `RecordID`, 
    `variableType`, 
    `Invoice Date`, 
    `Num`, 
    `Memo`, 
    `Name`, 
    `Item`, 
    `Qty`, 
    `Sales Price`, 
    `Amount`, 
    `F10`, 
    `Term`, 
    `First Month`, 
    `Last Month`, 
    `Monthly Revenue`, 
    `First Month Revenue`, 
    `Last Month Revenue`, 
    `2021-02-01`, 
    `2021-03-01`, 
    `2021-04-01`, 
    `2021-05-01`, 
    `2021-06-01`, 
    `2021-07-01`, 
    `2021-08-01`, 
    `2021-09-01`, 
    `2021-10-01`, 
    `2021-11-01`, 
    `2021-12-01`, 
    `2022-01-01`, 
    `2022-02-01`, 
    `2022-03-01`, 
    `2022-04-01`, 
    `2022-05-01`, 
    `2022-06-01`, 
    `2022-07-01`, 
    `2022-08-01`, 
    `2022-09-01`, 
    `2022-10-01`, 
    `2022-11-01`, 
    `2022-12-01`, 
    `2023-01-01`, 
    `2023-02-01`, 
    `2023-03-01`, 
    `2023-04-01`, 
    `2023-05-01`, 
    `2023-06-01`, 
    `2023-07-01`, 
    `2023-08-01`, 
    `2023-09-01`, 
    `2023-10-01`, 
    `2023-11-01`, 
    `2023-12-01`, 
    `2024-01-01`, 
    `2024-02-01`, 
    `2024-03-01`, 
    `2024-04-01`, 
    `2024-05-01`, 
    `2024-06-01`, 
    `2024-07-01`, 
    `2024-08-01`, 
    `2024-09-01`, 
    `2024-10-01`, 
    `2024-11-01`, 
    `2024-12-01`, 
    `2025-01-01`, 
    `2025-02-01`, 
    `2025-03-01`, 
    `2025-04-01`, 
    `2025-05-01`, 
    `2025-06-01`, 
    `2025-07-01`, 
    `2025-08-01`, 
    `2025-09-01`, 
    `2025-10-01`, 
    `2025-11-01`, 
    `2025-12-01`, 
    `CHECK`, 
    `Item2`, 
    `Term2`, 
    `Date Check`, 
    `variableDate`, 
    `Start Date Clean`, 
    `End Date Clean`)
  
  FROM Union_3225 AS in0

),

Filter_3167_to_Filter_3166 AS (

  SELECT * 
  
  FROM AlteryxSelect_3164 AS in0
  
  WHERE (
          (((NOT(`Start Date` IS NULL)) AND (NOT(`End Date` IS NULL))) AND (UPPER(Term) = UPPER('Annual')))
          AND (`Invoice Date` > '2022-02-09')
        )

),

AlteryxSelect_3168 AS (

  SELECT * EXCEPT (`F10`, 
         `First Month`, 
         `Last Month`, 
         `Monthly Revenue`, 
         `First Month Revenue`, 
         `Last Month Revenue`, 
         `2021-02-01`, 
         `2021-03-01`, 
         `2021-04-01`, 
         `2021-05-01`, 
         `2021-06-01`, 
         `2021-07-01`, 
         `2021-08-01`, 
         `2021-09-01`, 
         `2021-10-01`, 
         `2021-11-01`, 
         `2021-12-01`, 
         `2022-01-01`, 
         `2022-02-01`, 
         `2022-03-01`, 
         `2022-04-01`, 
         `2022-05-01`, 
         `2022-06-01`, 
         `2022-07-01`, 
         `2022-08-01`, 
         `2022-09-01`, 
         `2022-10-01`, 
         `2022-11-01`, 
         `2022-12-01`, 
         `2023-01-01`, 
         `2023-02-01`, 
         `2023-03-01`, 
         `2023-04-01`, 
         `2023-05-01`, 
         `2023-06-01`, 
         `2023-07-01`, 
         `2023-08-01`, 
         `2023-09-01`, 
         `2023-10-01`, 
         `2023-11-01`, 
         `2023-12-01`, 
         `2024-01-01`, 
         `2024-02-01`, 
         `2024-03-01`, 
         `2024-04-01`, 
         `2024-05-01`, 
         `2024-06-01`, 
         `2024-07-01`, 
         `2024-08-01`, 
         `2024-09-01`, 
         `2024-10-01`, 
         `2024-11-01`, 
         `2024-12-01`, 
         `CHECK`, 
         `Item2`, 
         `Term2`)
  
  FROM Filter_3167_to_Filter_3166 AS in0

),

Formula_3170_0 AS (

  SELECT 
    (TO_DATE(`Start Date`, 'yyyy-MM-dd')) AS Orig_ContractStart,
    (TO_DATE(`End Date`, 'yyyy-MM-dd')) AS Orig_ContractEnd,
    *
  
  FROM AlteryxSelect_3168 AS in0

),

Formula_3170_1 AS (

  SELECT 
    CAST(CAST(datediff(to_date(Orig_ContractEnd), to_date(Orig_ContractStart)) AS INT) AS INT) AS Orig_ContractTermDays,
    *
  
  FROM Formula_3170_0 AS in0

),

Formula_3170_2 AS (

  SELECT 
    CAST((Orig_ContractTermDays / 30.44) AS DOUBLE) AS ContractTermMonthsPreNorm,
    (
      TO_DATE(
        (
          CASE
            WHEN (
              (CAST((SUBSTRING(CAST(Orig_ContractEnd AS string), (((LENGTH(CAST(Orig_ContractEnd AS string))) - 2) + 1), 2)) AS string) IN ('13', '14', '15'))
              AND (CAST((SUBSTRING(CAST(Orig_ContractStart AS string), (((LENGTH(CAST(Orig_ContractStart AS string))) - 2) + 1), 2)) AS string) IN ('16', '17', '18'))
            )
              THEN (CONCAT((SUBSTRING(CAST(Orig_ContractStart AS string), 1, 7)), '-15'))
            ELSE Orig_ContractStart
          END
        ), 
        'yyyy-MM-dd')
    ) AS Orig_ContractStart,
    * EXCEPT (`orig_contractstart`)
  
  FROM Formula_3170_1 AS in0

),

Formula_3170_3 AS (

  SELECT 
    (
      TO_DATE(
        (
          CASE
            WHEN ((SUBSTRING(CAST(Orig_ContractStart AS string), (((LENGTH(CAST(Orig_ContractStart AS string))) - 2) + 1), 2)) <= '15')
              THEN (CONCAT((SUBSTRING(CAST(Orig_ContractStart AS string), 1, 7)), '-01'))
            ELSE (
              ADD_MONTHS(
                (DATE_FORMAT((CONCAT((SUBSTRING(CAST(Orig_ContractStart AS string), 1, 7)), '-01')), 'yyyy-MM-dd')), 
                1)
            )
          END
        ), 
        'yyyy-MM-dd')
    ) AS ContractStartDate,
    (
      TO_DATE(
        (
          CASE
            WHEN ((SUBSTRING(CAST(Orig_ContractEnd AS string), (((LENGTH(CAST(Orig_ContractEnd AS string))) - 2) + 1), 2)) <= '15')
              THEN (CONCAT((SUBSTRING(CAST(Orig_ContractEnd AS string), 1, 7)), '-01'))
            ELSE (ADD_MONTHS((DATE_FORMAT((CONCAT((SUBSTRING(CAST(Orig_ContractEnd AS string), 1, 7)), '-01')), 'yyyy-MM-dd')), 1))
          END
        ), 
        'yyyy-MM-dd')
    ) AS ContractEndDate,
    *
  
  FROM Formula_3170_2 AS in0

),

Formula_3170_4 AS (

  SELECT 
    (
      DATE_ADD(
        (
          CASE
            WHEN (ContractStartDate >= ContractEndDate)
              THEN (ADD_MONTHS(ContractStartDate, 1))
            ELSE ContractEndDate
          END
        ), 
        CAST(-1 AS INTEGER))
    ) AS ContractEndDate,
    * EXCEPT (`contractenddate`)
  
  FROM Formula_3170_3 AS in0

),

Formula_3170_5 AS (

  SELECT 
    CAST((CAST(datediff(to_date(ContractEndDate), to_date(ContractStartDate)) AS INT) / 30.44) AS DOUBLE) AS ContractTermMonthsFromDays,
    CAST(CAST((MONTHS_BETWEEN((TO_DATE((DATE_ADD(ContractEndDate, CAST(1 AS INTEGER))))), (TO_DATE(ContractStartDate)))) AS INTEGER) AS DOUBLE) AS ContractTermMonths,
    *
  
  FROM Formula_3170_4 AS in0

),

Formula_3170_6 AS (

  SELECT 
    CAST((Amount / ContractTermMonths) AS DOUBLE) AS Revenue,
    *
  
  FROM Formula_3170_5 AS in0

),

GenerateRows_3171 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Formula_3170_6'], 
      '[{"name": "Revenue", "dataType": "Double"}, {"name": "ContractTermMonthsFromDays", "dataType": "Double"}, {"name": "ContractTermMonths", "dataType": "Double"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "ContractStartDate", "dataType": "Date"}, {"name": "ContractTermMonthsPreNorm", "dataType": "Double"}, {"name": "Orig_ContractStart", "dataType": "Date"}, {"name": "Orig_ContractTermDays", "dataType": "Integer"}, {"name": "Orig_ContractEnd", "dataType": "Date"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "variableType", "dataType": "String"}, {"name": "Invoice Date", "dataType": "String"}, {"name": "Start Date", "dataType": "String"}, {"name": "End Date", "dataType": "String"}, {"name": "Num", "dataType": "String"}, {"name": "Memo", "dataType": "String"}, {"name": "Name", "dataType": "String"}, {"name": "Item", "dataType": "String"}, {"name": "Qty", "dataType": "Double"}, {"name": "Sales Price", "dataType": "String"}, {"name": "Amount", "dataType": "Double"}, {"name": "Term", "dataType": "String"}, {"name": "2025-01-01", "dataType": "Double"}, {"name": "2025-02-01", "dataType": "Double"}, {"name": "2025-03-01", "dataType": "Double"}, {"name": "2025-04-01", "dataType": "Double"}, {"name": "2025-05-01", "dataType": "Double"}, {"name": "2025-06-01", "dataType": "Double"}, {"name": "2025-07-01", "dataType": "Double"}, {"name": "2025-08-01", "dataType": "Double"}, {"name": "2025-09-01", "dataType": "Double"}, {"name": "2025-10-01", "dataType": "Double"}, {"name": "2025-11-01", "dataType": "Double"}, {"name": "2025-12-01", "dataType": "Double"}, {"name": "Date Check", "dataType": "Boolean"}, {"name": "variableDate", "dataType": "String"}, {"name": "2026-01-01", "dataType": "Double"}, {"name": "2026-02-01", "dataType": "Double"}, {"name": "2026-03-01", "dataType": "Double"}, {"name": "2026-04-01", "dataType": "Double"}, {"name": "2026-05-01", "dataType": "Double"}, {"name": "2026-06-01", "dataType": "Double"}, {"name": "2026-07-01", "dataType": "Double"}, {"name": "2026-08-01", "dataType": "Double"}, {"name": "2026-09-01", "dataType": "Double"}, {"name": "2026-10-01", "dataType": "Double"}, {"name": "2026-11-01", "dataType": "Double"}, {"name": "2026-12-01", "dataType": "Double"}, {"name": "F91", "dataType": "Double"}]', 
      'date_format(concat(substring(payload.contractstartdate, 1, 7), "-01"), "yyyy-MM-dd")', 
      '((RevMonth <= payload.contractenddate) AND (RevMonth <= "2025-01-01"))', 
      'add_months(RevMonth, 1)', 
      'RevMonth', 
      '100', 
      'recursive'
    )
  }}

),

Filter_3169 AS (

  SELECT * 
  
  FROM GenerateRows_3171 AS in0
  
  WHERE (
          (RevMonth >= ContractStartDate)
          AND (RevMonth < to_date(substring(CAST(date_add(ContractEndDate, CAST(1 AS INT)) AS STRING), 1, 10)))
        )

),

Formula_3173_to_Formula_3174_0 AS (

  SELECT 
    CAST('QUORUM' AS string) AS Product,
    CAST('N/A' AS string) AS `Customer Segment`,
    CAST('N/A' AS string) AS SubCustSeg1,
    CAST('N/A' AS string) AS SubCustSeg2,
    CAST('N/A' AS string) AS SubCustSeg3,
    CAST('N/A' AS string) AS SubCustSeg4,
    CAST('N/A' AS string) AS SubCustSeg5,
    CAST('N/A' AS string) AS SubCustSeg6,
    *
  
  FROM Filter_3169 AS in0

),

AlteryxSelect_3175 AS (

  SELECT 
    CAST(RecordID AS string) AS RecordID,
    RevMonth AS `Revenue Period`,
    * EXCEPT (`RecordID`, `RevMonth`)
  
  FROM Formula_3173_to_Formula_3174_0 AS in0

),

Summarize_3172 AS (

  SELECT 
    SUM(Revenue) AS Revenue,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg6 AS SubCustSeg6,
    `Customer Segment` AS `Customer Segment`,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    `Revenue Period` AS `Revenue Period`,
    Product AS Product,
    SubCustSeg2 AS SubCustSeg2
  
  FROM AlteryxSelect_3175 AS in0
  
  GROUP BY 
    SubCustSeg5, 
    SubCustSeg1, 
    SubCustSeg6, 
    `Customer Segment`, 
    SubCustSeg3, 
    SubCustSeg4, 
    `Revenue Period`, 
    Product, 
    SubCustSeg2

),

Filter_3177 AS (

  SELECT * 
  
  FROM Summarize_3172 AS in0
  
  WHERE (
          (`Revenue Period` >= to_date('2018-01-01'))
          AND (`Revenue Period` <= to_date({{ var('User__Current_Period') }}))
        )

),

Formula_3180_0 AS (

  SELECT 
    CAST((Revenue * 12) AS DOUBLE) AS Revenue,
    * EXCEPT (`revenue`)
  
  FROM Filter_3177 AS in0

),

Formula_3181_0 AS (

  SELECT 
    CAST('Year to date' AS string) AS `Comparison Method`,
    *
  
  FROM Formula_3180_0 AS in0

),

Formula_3182_0 AS (

  SELECT 
    CAST('Month-over-Month' AS string) AS `Comparison Method`,
    *
  
  FROM Formula_3180_0 AS in0

),

Union_3183 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_3181_0', 'Formula_3182_0'], 
      [
        '[{"name": "Comparison Method", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "SubCustSeg5", "dataType": "String"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "Revenue Period", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}]', 
        '[{"name": "Comparison Method", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "SubCustSeg5", "dataType": "String"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "Revenue Period", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_3183
