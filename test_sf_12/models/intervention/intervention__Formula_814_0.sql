{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TOTAL_INF_CONDI_1676 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('intervention', 'TOTAL_INF_CONDI_1676') }}

),

AlteryxSelect_810 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`,
    YearMonthDay_ AS YearMonthDay_,
    V AS V,
    `Inflection Index CY` AS `Inflection Index CY`,
    `Inflected Indicator CY` AS `Inflected Indicator CY`,
    CAST(Avg_6_w_2monthRunout AS DOUBLE) AS Avg_6_w_2monthRunout,
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

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM AlteryxSelect_810 AS in0
  
  WHERE ((`Inflected Indicator CY` = 'Y') OR (`Inflected Indicator Pred` = 'Y'))

),

ProviderDetailR_1677 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('intervention', 'ProviderDetailR_1677') }}

),

Filter_1645 AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM ProviderDetailR_1677 AS in0
  
  WHERE (`Recapture Flag` = 'Not Captured')

),

AlteryxSelect_1646 AS (

  {#VisualGroup: STEP1#}
  SELECT `Member Individual BE Key` AS `Member Individual Business Entity Key`
  
  FROM Filter_1645 AS in0

),

Summarize_1647 AS (

  {#VisualGroup: STEP1#}
  SELECT DISTINCT `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`
  
  FROM AlteryxSelect_1646 AS in0

),

Union_1648 AS (

  {#VisualGroup: STEP1#}
  {{
    prophecy_basics.UnionByName(
      ['Filter_813', 'Summarize_1647'], 
      [
        '[{"name": "Member Individual Business Entity Key", "dataType": "String"}, {"name": "YearMonthDay_", "dataType": "String"}, {"name": "V", "dataType": "String"}, {"name": "Inflection Index CY", "dataType": "String"}, {"name": "Inflected Indicator CY", "dataType": "String"}, {"name": "Avg_6_w_2monthRunout", "dataType": "Double"}, {"name": "Avg_3_w_2monthRunout", "dataType": "String"}, {"name": "AVG_6_MONTH_BIN", "dataType": "String"}, {"name": "Inflected Indicator CM", "dataType": "String"}, {"name": "Inflected Indicator Last 90 Days", "dataType": "String"}, {"name": "Inflected Indicator Last 60 Days", "dataType": "String"}, {"name": "AVG_12_MONTH_BIN_PRED", "dataType": "String"}, {"name": "Inflection Index Pred", "dataType": "String"}, {"name": "Inflected Indicator Pred", "dataType": "String"}]', 
        '[{"name": "Member Individual Business Entity Key", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Sample_812 AS (

  {#VisualGroup: STEP1#}
  {{ prophecy_basics.Sample('Union_1648', ['Member Individual Business Entity Key'], 1002, 'firstN', 1) }}

),

Formula_814_0 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    CAST(1 AS DOUBLE) AS `MARA INDICATOR`,
    *
  
  FROM Sample_812 AS in0

)

SELECT *

FROM Formula_814_0
