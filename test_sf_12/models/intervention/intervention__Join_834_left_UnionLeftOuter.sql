{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TOTAL_INF_CONDI_1674 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('intervention', 'TOTAL_INF_CONDI_1674') }}

),

Join_822_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_822_left_UnionLeftOuter')}}

),

AlteryxSelect_828 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`,
    YearMonthDay_ AS YearMonthDay_,
    V AS V,
    Avg_TotalRVU AS Avg_TotalRVU,
    `Inflection Index CY` AS `Inflection Index CY`,
    `Inflected Indicator CY` AS `Inflected Indicator CY`,
    CAST(Avg_6_w_2monthRunout AS DOUBLE) AS Avg_6_w_2monthRunout,
    AVG_6_MONTH_BIN AS AVG_6_MONTH_BIN,
    `Inflected Indicator CM` AS `Inflected Indicator CM`,
    `Inflected Indicator Last 90 Days` AS `Inflected Indicator Last 90 Days`,
    `Inflected Indicator Last 60 Days` AS `Inflected Indicator Last 60 Days`,
    AVG_12_MONTH_BIN_PRED AS AVG_12_MONTH_BIN_PRED,
    `Inflection Index Pred` AS `Inflection Index Pred`,
    `Inflected Indicator Pred` AS `Inflected Indicator Pred`
  
  FROM TOTAL_INF_CONDI_1674 AS in0

),

Filter_831 AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM AlteryxSelect_828 AS in0
  
  WHERE ((`Inflected Indicator CY` IS NULL) AND (Avg_6_w_2monthRunout > 125))

),

Sample_830 AS (

  {#VisualGroup: STEP1#}
  {{ prophecy_basics.Sample('Filter_831', ['Member Individual Business Entity Key'], 1002, 'firstN', 1) }}

),

Formula_832_to_Formula_833_0 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    CAST(1 AS DOUBLE) AS `MARA INDICATOR`,
    CAST(1 AS string) AS PERSISTANT_HIGH_RISK,
    *
  
  FROM Sample_830 AS in0

),

Join_834_left_UnionLeftOuter AS (

  {#VisualGroup: STEP1#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`Member Individual Business Entity Key`)
  
  FROM Join_822_left_UnionLeftOuter AS in0
  LEFT JOIN Formula_832_to_Formula_833_0 AS in1
     ON (in0.`Member Individual Business Entity Key` = in1.`Member Individual Business Entity Key`)

)

SELECT *

FROM Join_834_left_UnionLeftOuter
