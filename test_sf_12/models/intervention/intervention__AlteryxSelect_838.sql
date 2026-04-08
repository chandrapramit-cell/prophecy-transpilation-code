{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_817 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_817')}}

),

Formula_837_0 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    CAST((
      CASE
        WHEN CAST((Right_YearMonthDay_ IS NULL) AS BOOLEAN)
          THEN YearMonthDay_
        ELSE Right_YearMonthDay_
      END
    ) AS string) AS YearMonthDay_,
    CAST((
      CASE
        WHEN CAST((Right_V IS NULL) AS BOOLEAN)
          THEN V
        ELSE Right_V
      END
    ) AS string) AS V,
    CAST((
      CASE
        WHEN CAST((`Right_Inflection Index CY` IS NULL) AS BOOLEAN)
          THEN `Inflection Index CY`
        ELSE `Right_Inflection Index CY`
      END
    ) AS string) AS `Inflection Index CY`,
    CAST((
      CASE
        WHEN CAST((`Right_Inflected Indicator CY` IS NULL) AS BOOLEAN)
          THEN `Inflected Indicator CY`
        ELSE `Right_Inflected Indicator CY`
      END
    ) AS string) AS `Inflected Indicator CY`,
    CAST((
      CASE
        WHEN CAST((Right_AVG_6_MONTH_BIN IS NULL) AS BOOLEAN)
          THEN AVG_6_MONTH_BIN
        ELSE Right_AVG_6_MONTH_BIN
      END
    ) AS string) AS AVG_6_MONTH_BIN,
    CAST((
      CASE
        WHEN CAST((`Right_Inflected Indicator CM` IS NULL) AS BOOLEAN)
          THEN `Inflected Indicator CM`
        ELSE `Right_Inflected Indicator CM`
      END
    ) AS string) AS `Inflected Indicator CM`,
    CAST((
      CASE
        WHEN CAST((`Right_Inflected Indicator Last 90 Days` IS NULL) AS BOOLEAN)
          THEN `Inflected Indicator Last 90 Days`
        ELSE `Right_Inflected Indicator Last 90 Days`
      END
    ) AS string) AS `Inflected Indicator Last 90 Days`,
    CAST((
      CASE
        WHEN CAST((`Right_Inflected Indicator Last 60 Days` IS NULL) AS BOOLEAN)
          THEN `Right_Inflected Indicator Last 60 Days`
        ELSE `Right_Inflected Indicator Last 60 Days`
      END
    ) AS string) AS `Inflected Indicator Last 60 Days`,
    CAST((
      CASE
        WHEN CAST((Right_Avg_6_w_2monthRunout IS NULL) AS BOOLEAN)
          THEN Avg_6_w_2monthRunout
        ELSE Right_Avg_6_w_2monthRunout
      END
    ) AS DOUBLE) AS Avg_6_w_2monthRunout,
    CAST((
      CASE
        WHEN CAST((Right_AVG_12_MONTH_BIN_PRED IS NULL) AS BOOLEAN)
          THEN AVG_12_MONTH_BIN_PRED
        ELSE Right_AVG_12_MONTH_BIN_PRED
      END
    ) AS string) AS AVG_12_MONTH_BIN_PRED,
    CAST((
      CASE
        WHEN CAST((`Right_Inflection Index Pred` IS NULL) AS BOOLEAN)
          THEN `Inflection Index Pred`
        ELSE `Right_Inflection Index Pred`
      END
    ) AS string) AS `Inflection Index Pred`,
    CAST((
      CASE
        WHEN CAST((`Right_Inflected Indicator Pred` IS NULL) AS BOOLEAN)
          THEN `Inflected Indicator Pred`
        ELSE `Right_Inflected Indicator Pred`
      END
    ) AS string) AS `Inflected Indicator Pred`,
    * EXCEPT (`inflected indicator cy`, 
    `avg_6_month_bin`, 
    `inflected indicator last 60 days`, 
    `avg_12_month_bin_pred`, 
    `inflection index pred`, 
    `avg_6_w_2monthrunout`, 
    `inflection index cy`, 
    `v`, 
    `inflected indicator cm`, 
    `inflected indicator last 90 days`, 
    `inflected indicator pred`, 
    `yearmonthday_`)
  
  FROM Union_817 AS in0

),

AlteryxSelect_838 AS (

  {#VisualGroup: STEP1#}
  SELECT * EXCEPT (`Right_Member Individual Business Entity Key`, 
         `Right_YearMonthDay_`, 
         `Right_V`, 
         `Right_Inflection Index CY`, 
         `Right_Inflected Indicator CY`, 
         `Right_Avg_6_w_2monthRunout`, 
         `Right_AVG_6_MONTH_BIN`, 
         `Right_Inflected Indicator CM`, 
         `Right_Inflected Indicator Last 90 Days`, 
         `Right_Inflected Indicator Last 60 Days`, 
         `Right_MARA INDICATOR`)
  
  FROM Formula_837_0 AS in0

)

SELECT *

FROM AlteryxSelect_838
