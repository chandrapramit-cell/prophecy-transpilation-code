{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Filter_844_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Filter_844_3118')}}

),

Filter_843_3118_reject AS (

  SELECT * 
  
  FROM Filter_844_3118 AS in0
  
  WHERE (
          ((Stage IS NULL) OR ((NOT(Stage IS NULL)) IS NULL))
          AND (
                (NOT((UPPER(Origin) = UPPER('Orders&OrdersProcessed')) AND (`ARR Period` > StaticHistoryMonth)))
                OR (((UPPER(Origin) = UPPER('Orders&OrdersProcessed')) AND (`ARR Period` > StaticHistoryMonth)) IS NULL)
              )
        )

),

Formula_845_3118_1 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Formula_845_3118_1')}}

),

AlteryxSelect_1271_3118 AS (

  SELECT * EXCEPT (`RecordID`)
  
  FROM Formula_845_3118_1 AS in0

),

Union_868_3118 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_843_3118_reject', 'AlteryxSelect_1271_3118'], 
      [
        '[{"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "Amount", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Activated Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]', 
        '[{"name": "TCV", "dataType": "Double"}, {"name": "Open Renewal Flag", "dataType": "Boolean"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "Amount", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Activated Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_859_3118 AS (

  SELECT * 
  
  FROM Union_868_3118 AS in0
  
  WHERE ((NOT(ContractStartDate IS NULL)) AND (NOT(ContractEndDate IS NULL)))

),

Formula_862_3118_0 AS (

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
  
  FROM Filter_859_3118 AS in0

),

Formula_862_3118_1 AS (

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
  
  FROM Formula_862_3118_0 AS in0

),

GenerateRows_861_3118 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Formula_862_3118_1'], 
      '[{"name": "ARR", "dataType": "Double"}, {"name": "ContractTermMonths", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "Amount", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Activated Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}, {"name": "Open Renewal Flag", "dataType": "Boolean"}]', 
      'last_day(payload.ContractStartDate)', 
      '((ARRMonth <= payload.ContractEndDate) AND (ARRMonth <= concat(regexp_replace(regexp_replace(format_number(CAST(year(current_timestamp()) AS DOUBLE), 0), ",", "__THS__"), "__THS__", ""), "-12-31")))', 
      'last_day(add_months(ARRMonth, 1))', 
      'ARRMonth', 
      '100', 
      'recursive'
    )
  }}

),

Filter_863_3118_to_Filter_870_3118 AS (

  SELECT * 
  
  FROM GenerateRows_861_3118 AS in0
  
  WHERE (
          (
            (ARRMonth >= ContractStartDate)
            AND (ARRMonth < to_date(substring(CAST(date_add(ContractEndDate, CAST(1 AS INT)) AS STRING), 1, 10)))
          )
          AND (
                NOT (coalesce(contains(lower(Stage), lower('Closed')), false))
                OR (`Actual Closed Date` > StaticHistoryMonth)
              )
        )

)

SELECT *

FROM Filter_863_3118_to_Filter_870_3118
