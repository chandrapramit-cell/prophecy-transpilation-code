{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_ACCOUNT_COMPARE AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_account_update__EXP_ACCOUNT_COMPARE')}}

),

EXP_ACCOUNT_CHANGE_sp AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_account_update', 'EXP_ACCOUNT_CHANGE_sp') }}

),

EXP_ACCOUNT_CHANGE_reformat AS (

  SELECT 
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
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
          THEN (ERROR('No Business Date found on dbattriute'))
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
      CASE
        WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1)
          THEN output_sp
        ELSE NULL
      END
    ) AS feed_update_id,
    'U' AS SOURCE_SYSTEM_CODE,
    0 AS SOURCE_TYPE,
    270 AS HOLDS_CLASS,
    o_comp_acc_type1 AS o_comp_acc_type1,
    o_comp_source_code1 AS o_comp_source_code1,
    o_comp_store_number1 AS o_comp_store_number1,
    o_compare_status1 AS o_compare_status1,
    o_compare_status_date1 AS o_compare_status_date1,
    (
      CASE
        WHEN (O_COMPARE_STATUS1 = 1)
          THEN (
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
                THEN (ERROR('No Business Date found on dbattriute'))
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
          )
        ELSE DATE_LAST_ACC_STATUS_CHANGE_TGT1
      END
    ) AS o_DATE_LAST_ACC_STATUS_CHANGE_TGT,
    (
      (((o_comp_acc_type1 + o_comp_store_number1) + o_comp_source_code1) + o_compare_status1)
      + o_compare_status_date1
    ) AS TOTAL_COMPARE,
    prophecy_sk AS prophecy_sk,
    DATE_LAST_ACC_STATUS_CHANGE_TGT1 AS DATE_LAST_ACC_STATUS_CHANGE_TGT1,
    lookup_string AS lookup_string,
    business_date_string__find_business_date_lkp_1 AS business_date_string__find_business_date_lkp_1,
    LOOKUP_VARIABLE_3 AS LOOKUP_VARIABLE_3,
    output_sp AS output_sp
  
  FROM EXP_ACCOUNT_CHANGE_sp AS in0

),

RTR_ACCOUNT_JOIN AS (

  SELECT 
    in0.ACCID_ACCOUNT AS ACCID_ACCOUNT,
    in0.APPLICATION_NUMBER AS APPLICATION_NUMBER,
    in0.o_PRODUCT_SRC AS o_PRODUCT_SRC,
    in0.APPLICATION_STORE_NUMBER AS APPLICATION_STORE_NUMBER,
    in0.o_APPLICATION_STATUS_SRC AS o_APPLICATION_STATUS_SRC,
    in1.feed_update_id AS feed_update_id,
    in0.MERGE_CLID AS MERGE_CLID,
    in1.SOURCE_TYPE AS SOURCE_TYPE,
    in1.o_DATE_LAST_ACC_STATUS_CHANGE_TGT AS o_DATE_LAST_ACC_STATUS_CHANGE_TGT,
    in0.CLID_CLIENT_ALIAS AS CLID_CLIENT_ALIAS,
    in1.SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE,
    in0.APPLICATION_SOURCE_CODE AS APPLICATION_SOURCE_CODE,
    in0.prophecy_sk AS prophecy_sk,
    in1.business_date AS business_date,
    in1.TOTAL_COMPARE AS TOTAL_COMPARE,
    in1.HOLDS_CLASS AS HOLDS_CLASS,
    in0.ACCID_ACCOUNT_ALIAS AS ACCID_ACCOUNT_ALIAS,
    in0.LAST_ACTION_DATE_SRC AS LAST_ACTION_DATE_SRC
  
  FROM EXP_ACCOUNT_COMPARE AS in0
  INNER JOIN EXP_ACCOUNT_CHANGE_reformat AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

RTR_ACCOUNT_JOIN_EXPR_83 AS (

  SELECT 
    APPLICATION_SOURCE_CODE AS APPLICATION_SOURCE_CODE,
    ACCID_ACCOUNT AS ACCID_ACCOUNT,
    ACCID_ACCOUNT_ALIAS AS ACCID_ACCOUNT_ALIAS,
    o_PRODUCT_SRC AS o_PRODUCT_SRC,
    o_APPLICATION_STATUS_SRC AS o_APPLICATION_STATUS_SRC,
    LAST_ACTION_DATE_SRC AS LAST_ACTION_DATE_SRC,
    CLID_CLIENT_ALIAS AS CLID_CLIENT_ALIAS,
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    APPLICATION_STORE_NUMBER AS APPLICATION_STORE_NUMBER,
    MERGE_CLID AS MERGE_CLID,
    prophecy_sk AS prophecy_sk,
    business_date AS BUSINESS_DATE,
    feed_update_id AS feed_update_id,
    o_DATE_LAST_ACC_STATUS_CHANGE_TGT AS o_DATE_LAST_ACC_STATUS_CHANGE,
    SOURCE_TYPE AS SOURCE_TYPE,
    HOLDS_CLASS AS HOLDS_CLASS,
    SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE,
    TOTAL_COMPARE AS TOTAL_COMPARE
  
  FROM RTR_ACCOUNT_JOIN AS in0

)

SELECT *

FROM RTR_ACCOUNT_JOIN_EXPR_83
