{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_GET_DATES_AND_FEED_NUMBER_sp AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_account_history_update', 'EXP_GET_DATES_AND_FEED_NUMBER_sp') }}

),

EXP_GET_DATES_AND_FEED_NUMBER_reformat AS (

  SELECT 
    ACCID_ACCOUNT_ALIAS AS ACCID_ACCOUNT_ALIAS,
    ACCID_TGT AS ACCID_TGT,
    APPLICATION_STORE_NUMBER_SRC AS APPLICATION_STORE_NUMBER_SRC,
    STATUS_DATE_SRC AS STATUS_DATE_SRC,
    STATUS_SRC AS STATUS_SRC,
    DOMICILE_BRANCH_TGT AS DOMICILE_BRANCH_TGT,
    STATUS_TGT AS STATUS_TGT,
    STATUS_DATE_TGT AS STATUS_DATE_TGT,
    EFROM_TGT AS EFROM_TGT,
    total_compare AS total_compare,
    (
      CASE
        WHEN CAST((
          (
            CASE
              WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
              ELSE CAST(NULL AS STRING)
            END
          ) IS NULL
        ) AS BOOL)
          THEN NULL
        ELSE (
          PARSE_TIMESTAMP(
            '%Y%m%d', 
            CAST(SUBSTRING(
              (
                CASE
                  WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                    THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
                  ELSE CAST(NULL AS STRING)
                END
              ), 
              0, 
              8) AS STRING))
        )
      END
    ) AS business_date,
    (
      DATE_ADD(
        (
          CASE
            WHEN CAST((
              (
                CASE
                  WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                    THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
                  ELSE CAST(NULL AS STRING)
                END
              ) IS NULL
            ) AS BOOL)
              THEN NULL
            ELSE (
              PARSE_TIMESTAMP(
                '%Y%m%d', 
                CAST(SUBSTRING(
                  (
                    CASE
                      WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                        THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
                      ELSE CAST(NULL AS STRING)
                    END
                  ), 
                  0, 
                  8) AS STRING))
            )
          END
        ), 
        INTERVAL -1 DAY)
    ) AS business_date_minus_1,
    (
      CASE
        WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1)
          THEN output_sp
        ELSE NULL
      END
    ) AS feed_update_id,
    lookup_string AS lookup_string,
    business_date_string__find_business_date_lkp_1 AS business_date_string__find_business_date_lkp_1,
    LOOKUP_VARIABLE_3 AS LOOKUP_VARIABLE_3,
    output_sp AS output_sp
  
  FROM EXP_GET_DATES_AND_FEED_NUMBER_sp AS in0

),

EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1 AS (

  SELECT 
    ACCID_TGT AS ACCID_TGT,
    APPLICATION_STORE_NUMBER_SRC AS APPLICATION_STORE_NUMBER_SRC,
    STATUS_SRC AS STATUS_SRC,
    STATUS_DATE_SRC AS STATUS_DATE_SRC,
    total_compare AS total_compare,
    business_date AS BUSINESS_DATE,
    business_date_minus_1 AS BUSINESS_DATE_MINUS_1,
    ACCID_ACCOUNT_ALIAS AS ACCID_ACCOUNT_ALIAS,
    EFROM_TGT AS EFROM_TGT,
    feed_update_id AS feed_update_id,
    lookup_string AS lookup_string,
    business_date_string__find_business_date_lkp_1 AS business_date_string__find_business_date_lkp_1,
    DOMICILE_BRANCH_TGT AS DOMICILE_BRANCH_TGT,
    STATUS_TGT AS STATUS_TGT,
    STATUS_DATE_TGT AS STATUS_DATE_TGT
  
  FROM EXP_GET_DATES_AND_FEED_NUMBER_reformat AS in0

)

SELECT *

FROM EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1
