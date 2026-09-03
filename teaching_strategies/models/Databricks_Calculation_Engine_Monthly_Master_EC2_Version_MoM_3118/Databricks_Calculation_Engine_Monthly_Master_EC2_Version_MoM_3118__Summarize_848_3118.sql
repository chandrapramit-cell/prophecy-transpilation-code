{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH table_3118_Input_macro_ip AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_3118_Input_macro_ip') }}

),

Sample_847_3118 AS (

  {{
    prophecy_basics.Sample(
      ['table_3118_Input_macro_ip'], 
      '[{"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "ARR Period", "dataType": "Date"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractEndDate", "dataType": "Date"}, {"name": "Amount", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Activated Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "ContractStartDate", "dataType": "Date"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

Formula_846_3118_0 AS (

  SELECT 
    (LAST_DAY(CAST((ADD_MONTHS('2018-01-31', '{{ var('iteration_number') }}')) AS DATE))) AS StaticHistoryMonth,
    CAST((
      CAST((
        MONTHS_BETWEEN(
          (TO_DATE((DATE_ADD((DATE_TRUNC('month', CURRENT_DATE)), CAST(-1 AS INTEGER))))), 
          (TO_DATE('2018-01-01')))
      ) AS INTEGER)
      + 1
    ) AS DOUBLE) AS MaxIteration,
    *
  
  FROM Sample_847_3118 AS in0

),

Formula_846_3118_1 AS (

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
    *
  
  FROM Formula_846_3118_0 AS in0

),

Summarize_848_3118 AS (

  SELECT 
    DISTINCT StaticHistoryMonth AS StaticHistoryMonth,
    MaxIteration AS MaxIteration,
    StaticHistoryYearEnd AS StaticHistoryYearEnd
  
  FROM Formula_846_3118_1 AS in0

)

SELECT *

FROM Summarize_848_3118
