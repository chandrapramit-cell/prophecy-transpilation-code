{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TOTAL_INF_CONDI_1676 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'TOTAL_INF_CONDI_1676_ref') }}

),

AlteryxSelect_810 AS (

  SELECT 
    `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`,
    YearMonthDay_ AS YearMonthDay_,
    V AS V,
    `Inflection Index CY` AS `Inflection Index CY`,
    `Inflected Indicator CY` AS `Inflected Indicator CY`,
    CAST(Avg_6_w_2monthRunout AS FLOAT64) AS Avg_6_w_2monthRunout,
    Avg_3_w_2monthRunout AS Avg_3_w_2monthRunout,
    AVG_6_MONTH_BIN AS AVG_6_MONTH_BIN,
    `Inflected Indicator CM` AS `Inflected Indicator CM`,
    `Inflected Indicator Last 90 Days` AS `Inflected Indicator Last 90 Days`,
    `Inflected Indicator Last 60 Days` AS `Inflected Indicator Last 60 Days`,
    AVG_12_MONTH_BIN_PRED AS AVG_12_MONTH_BIN_PRED,
    `Inflection Index Pred` AS `Inflection Index Pred`,
    `Inflected Indicator Pred` AS `Inflected Indicator Pred`
  
  FROM TOTAL_INF_CONDI_1676 AS in0

),

Filter_813 AS (

  SELECT * 
  
  FROM AlteryxSelect_810 AS in0
  
  WHERE ((`Inflected Indicator CY` = 'Y') OR (`Inflected Indicator Pred` = 'Y'))

),

ProviderDetailR_1677 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'ProviderDetailR_1677_ref') }}

),

Filter_1645 AS (

  SELECT * 
  
  FROM ProviderDetailR_1677 AS in0
  
  WHERE (`Recapture Flag` = 'Not Captured')

),

AlteryxSelect_1646 AS (

  SELECT `Member Individual BE Key` AS `Member Individual Business Entity Key`
  
  FROM Filter_1645 AS in0

),

Summarize_1647 AS (

  SELECT DISTINCT `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`
  
  FROM AlteryxSelect_1646 AS in0

),

Union_1648 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_813', 'Summarize_1647'], 
      [
        '[{"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "YearMonthDay_", "dataType": "String"}, {"name": "V", "dataType": "String"}, {"name": "Inflection Index CY", "dataType": "String"}, {"name": "Inflected Indicator CY", "dataType": "String"}, {"name": "Avg_6_w_2monthRunout", "dataType": "Float"}, {"name": "Avg_3_w_2monthRunout", "dataType": "String"}, {"name": "AVG_6_MONTH_BIN", "dataType": "String"}, {"name": "Inflected Indicator CM", "dataType": "String"}, {"name": "Inflected Indicator Last 90 Days", "dataType": "String"}, {"name": "Inflected Indicator Last 60 Days", "dataType": "String"}, {"name": "AVG_12_MONTH_BIN_PRED", "dataType": "String"}, {"name": "Inflection Index Pred", "dataType": "String"}, {"name": "Inflected Indicator Pred", "dataType": "String"}]', 
        '[{"name": "Member Individual Business Entity Key", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Sort_811 AS (

  SELECT * 
  
  FROM Union_1648 AS in0
  
  ORDER BY CAST(`Member Individual Business Entity Key` AS STRING) ASC, CAST(YearMonthDay_ AS STRING) DESC, CAST(`Inflected Indicator CM` AS STRING) DESC, CAST(`Inflected Indicator Last 60 Days` AS STRING) DESC, CAST(`Inflected Indicator Last 90 Days` AS STRING) DESC, CAST(`Inflected Indicator CY` AS STRING) DESC

),

Sample_812 AS (

  {{ prophecy_basics.Sample('Sort_811', ['Member Individual Business Entity Key'], 1002, 'firstN', 1) }}

),

Formula_814_0 AS (

  SELECT 
    1 AS `MARA INDICATOR`,
    *
  
  FROM Sample_812 AS in0

)

SELECT *

FROM Formula_814_0
