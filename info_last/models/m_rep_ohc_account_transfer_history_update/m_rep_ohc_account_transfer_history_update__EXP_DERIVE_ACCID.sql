{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CONV_sp AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_ohc_account_transfer_history_update', 'EXP_CONV_sp') }}

),

SOURCE_LKP_ACCOUNT_TRANSFER_HISTORY_ACCID AS (

  SELECT 
    /*+ parallel(A)*/
    A.ACCID AS ACCID,
    A.CURR_ACCOUNT_ALIAS_ID AS CURR_ACCOUNT_ALIAS_ID
  
  FROM ACCOUNT_TRANSFER_HISTORY AS A
  
  WHERE A.ETO IS NULL

),

EXP_CONV_reformat AS (

  SELECT 
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
    SUBSTRING(OLD_ACCOUNT_NUMBER_IN, 3, 16) AS OLD_ACCOUNT_NUMBER,
    SUBSTRING(NEW_ACCOUNT_NUMBER_IN, 3, 16) AS NEW_ACCOUNT_NUMBER,
    (
      CASE
        WHEN (CAST(SUBSTRING((LTRIM((RTRIM(TRANSFER_EFFECTIVE_DATE_IN)))), 0, 7) AS STRING) = '0000000')
          THEN NULL
        WHEN (SUBSTRING((LTRIM((RTRIM(TRANSFER_EFFECTIVE_DATE_IN)))), 0, 7) IS NULL)
          THEN NULL
        WHEN (
          SUBSTRING((LTRIM((RTRIM(TRANSFER_EFFECTIVE_DATE_IN)))), 0, 7) = CAST((
            NOT(
              CASE
                WHEN (
                  (REGEXP_CONTAINS(SUBSTRING((LTRIM((RTRIM(TRANSFER_EFFECTIVE_DATE_IN)))), 0, 7), '^[0-9]+$'))
                  OR ((LENGTH(CAST(SUBSTRING((LTRIM((RTRIM(TRANSFER_EFFECTIVE_DATE_IN)))), 0, 7) AS STRING))) = 0)
                )
                  THEN TRUE
                ELSE FALSE
              END)
          ) AS STRING)
        )
          THEN NULL
        ELSE (
          DATE_ADD(
            (
              PARSE_TIMESTAMP(
                '%Y%m%d', 
                CAST((CONCAT(SUBSTRING(SUBSTRING((LTRIM((RTRIM(TRANSFER_EFFECTIVE_DATE_IN)))), 0, 7), 0, 4), '0101')) AS STRING))
            ), 
            INTERVAL (CAST(SUBSTRING(SUBSTRING((LTRIM((RTRIM(TRANSFER_EFFECTIVE_DATE_IN)))), 0, 7), 4, 3) AS FLOAT64) - 1) DAY)
        )
      END
    ) AS TRANSFER_EFFECTIVE_DATE,
    'G' AS SOURCE_SYSTEM_CODE,
    prophecy_sk AS prophecy_sk,
    OLD_ACCOUNT_NUMBER_IN AS OLD_ACCOUNT_NUMBER_IN,
    NEW_ACCOUNT_NUMBER_IN AS NEW_ACCOUNT_NUMBER_IN,
    TRANSFER_EFFECTIVE_DATE_IN AS TRANSFER_EFFECTIVE_DATE_IN,
    lookup_string AS lookup_string,
    business_date_string__find_business_date_lkp_1 AS business_date_string__find_business_date_lkp_1,
    LOOKUP_VARIABLE_3 AS LOOKUP_VARIABLE_3,
    output_sp AS output_sp
  
  FROM EXP_CONV_sp AS in0

),

SOURCE_LKP_OLD_ACCOUNT_ALIAS AS (

  SELECT 
    /*+ parallel(A)*/
    A.ACCID AS ACCID,
    A.ACCOUNT_ALIAS_ID AS ACCOUNT_ALIAS_ID
  
  FROM ACCOUNT_ALIAS AS A
  
  WHERE A.ETO IS NULL AND A.SOURCE_SYSTEM_CODE = 'G'

),

EXP_DERIVE_ACCID_JOIN_merged AS (

  SELECT 
    in0.OLD_ACCOUNT_NUMBER AS OLD_ACCOUNT_NUMBER,
    in1.ACCID AS ACCID,
    in2.NEW_ACCOUNT_NUMBER AS NEW_ACCOUNT_NUMBER,
    in2.TRANSFER_EFFECTIVE_DATE AS TRANSFER_EFFECTIVE_DATE,
    in2.feed_update_id AS feed_update_id,
    in2.business_date_minus_1 AS business_date_minus_1,
    in4.CURR_ACCOUNT_ALIAS_ID AS CURR_ACCOUNT_ALIAS_ID,
    in2.SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE,
    in0.prophecy_sk AS prophecy_sk,
    in2.business_date AS business_date,
    in1.ACCOUNT_ALIAS_ID AS ACCOUNT_ALIAS_ID
  
  FROM EXP_CONV_reformat AS in0
  INNER JOIN SOURCE_LKP_OLD_ACCOUNT_ALIAS AS in1
     ON (in1.ACCOUNT_ALIAS_ID = in0.OLD_ACCOUNT_NUMBER)
  INNER JOIN EXP_CONV_reformat AS in2
     ON in0.prophecy_sk = in2.prophecy_sk
  INNER JOIN EXP_CONV_reformat AS in3
     ON (in2.prophecy_sk = in3.prophecy_sk)
  INNER JOIN SOURCE_LKP_ACCOUNT_TRANSFER_HISTORY_ACCID AS in4
     ON (in4.CURR_ACCOUNT_ALIAS_ID = in3.OLD_ACCOUNT_NUMBER)

),

EXP_DERIVE_ACCID AS (

  SELECT 
    business_date AS business_date,
    business_date_minus_1 AS business_date_minus_1,
    feed_update_id AS feed_update_id,
    ACCID AS PRIMARY_ACCID,
    OLD_ACCOUNT_NUMBER AS OLD_ACCOUNT_NUMBER,
    NEW_ACCOUNT_NUMBER AS NEW_ACCOUNT_NUMBER,
    TRANSFER_EFFECTIVE_DATE AS TRANSFER_EFFECTIVE_DATE,
    SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_DERIVE_ACCID_JOIN_merged AS in0

)

SELECT *

FROM EXP_DERIVE_ACCID
