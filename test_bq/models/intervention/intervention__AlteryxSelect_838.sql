{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Union_817 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_817')}}

),

Formula_837_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Right_YearMonthDay_ IS NULL)
          THEN YearMonthDay_
        ELSE Right_YearMonthDay_
      END
    ) AS STRING) AS YearMonthDay_,
    CAST((
      CASE
        WHEN (Right_V IS NULL)
          THEN V
        ELSE Right_V
      END
    ) AS STRING) AS V,
    CAST((
      CASE
        WHEN (`Right_Inflection Index CY` IS NULL)
          THEN `Inflection Index CY`
        ELSE `Right_Inflection Index CY`
      END
    ) AS STRING) AS `Inflection Index CY`,
    CAST((
      CASE
        WHEN (`Right_Inflected Indicator CY` IS NULL)
          THEN `Inflected Indicator CY`
        ELSE `Right_Inflected Indicator CY`
      END
    ) AS STRING) AS `Inflected Indicator CY`,
    CAST((
      CASE
        WHEN (Right_AVG_6_MONTH_BIN IS NULL)
          THEN AVG_6_MONTH_BIN
        ELSE Right_AVG_6_MONTH_BIN
      END
    ) AS STRING) AS AVG_6_MONTH_BIN,
    CAST((
      CASE
        WHEN (`Right_Inflected Indicator CM` IS NULL)
          THEN `Inflected Indicator CM`
        ELSE `Right_Inflected Indicator CM`
      END
    ) AS STRING) AS `Inflected Indicator CM`,
    CAST((
      CASE
        WHEN (`Right_Inflected Indicator Last 90 Days` IS NULL)
          THEN `Inflected Indicator Last 90 Days`
        ELSE `Right_Inflected Indicator Last 90 Days`
      END
    ) AS STRING) AS `Inflected Indicator Last 90 Days`,
    CAST(`Right_Inflected Indicator Last 60 Days` AS STRING) AS `Inflected Indicator Last 60 Days`,
    (
      CASE
        WHEN (Right_Avg_6_w_2monthRunout IS NULL)
          THEN Avg_6_w_2monthRunout
        ELSE Right_Avg_6_w_2monthRunout
      END
    ) AS Avg_6_w_2monthRunout,
    CAST((
      CASE
        WHEN (Right_AVG_12_MONTH_BIN_PRED IS NULL)
          THEN AVG_12_MONTH_BIN_PRED
        ELSE Right_AVG_12_MONTH_BIN_PRED
      END
    ) AS STRING) AS AVG_12_MONTH_BIN_PRED,
    CAST((
      CASE
        WHEN (`Right_Inflection Index Pred` IS NULL)
          THEN `Inflection Index Pred`
        ELSE `Right_Inflection Index Pred`
      END
    ) AS STRING) AS `Inflection Index Pred`,
    CAST((
      CASE
        WHEN (`Right_Inflected Indicator Pred` IS NULL)
          THEN `Inflected Indicator Pred`
        ELSE `Right_Inflected Indicator Pred`
      END
    ) AS STRING) AS `Inflected Indicator Pred`,
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
