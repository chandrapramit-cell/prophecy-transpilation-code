{{
  config({    
    "materialized": "table",
    "alias": "CLOSE_CARD_DETAIL",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CONSTRUCT_CARD_DETAIL_sp AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_ohc_card_detail_closure', 'EXP_CONSTRUCT_CARD_DETAIL_sp') }}

),

SOURCE_LKP_ACCID_ON_CARD_DETAIL AS (

  SELECT 
    /*+parallel(C)*/
    C.EFROM AS EFROM,
    C.ACCID AS ACCID,
    C.CARD_NUMBER AS CARD_NUMBER
  
  FROM CARD_DETAIL AS C
  
  WHERE C.ETO IS NULL

),

EXP_CONSTRUCT_CARD_DETAIL_reformat AS (

  SELECT 
    CARD_NUMBER AS CARD_NUMBER,
    BLOCK_CODE AS BLOCK_CODE,
    CARDHOLDER_FLAG AS CARDHOLDER_FLAG,
    ACTV_RQD_CURR AS ACTV_RQD_CURR,
    ACTV_RQD_PREV AS ACTV_RQD_PREV,
    (
      LTRIM(
        (
          RTRIM(
            (
              CASE
                WHEN (EMBOSSER_NAME1 IS NOT NULL)
                  THEN (
                    CONCAT(
                      SUBSTRING(EMBOSSER_NAME1, 0, (0 - 1)), 
                      ' ', 
                      SUBSTRING(EMBOSSER_NAME1, (1 - 1), ((0 - 0) - 1)), 
                      ' ', 
                      SUBSTRING(EMBOSSER_NAME1, (1 - 1), (LENGTH(EMBOSSER_NAME1))))
                  )
                ELSE NULL
              END
            ))
        ))
    ) AS O_EMBOSSER_NAME,
    PRY_SDY_IND AS PRY_SDY_IND,
    CURR_PREV_IND AS CURR_PREV_IND,
    CAST(ACTUAL_PROGRAM_ID AS FLOAT64) AS O_ACTUAL_PROGRAM_ID,
    CAST(PREVIOUS_PROGRAM_ID AS FLOAT64) AS O_PREVIOUS_PROGRAM_ID,
    ACCID AS ACCID,
    (
      CASE
        WHEN (ACTIVE_DATE IS NULL)
          THEN NULL
        WHEN (ACTIVE_DATE = '0000000')
          THEN NULL
        WHEN (ACTIVE_DATE = '9999999')
          THEN NULL
        WHEN (
          ACTIVE_DATE = CAST((
            NOT(
              CASE
                WHEN ((REGEXP_CONTAINS(ACTIVE_DATE, '^[0-9]+$')) OR ((LENGTH(ACTIVE_DATE)) = 0))
                  THEN TRUE
                ELSE FALSE
              END)
          ) AS STRING)
        )
          THEN NULL
        ELSE (
          DATE_ADD(
            (PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(SUBSTRING(ACTIVE_DATE, 0, 4), '0101')) AS STRING))), 
            INTERVAL (CAST(SUBSTRING(ACTIVE_DATE, 4, 3) AS FLOAT64) - 1) DAY)
        )
      END
    ) AS O_ACTIVE_DATE,
    (
      CASE
        WHEN (EXPIRY_DATE <> '0000000')
          THEN (
            CASE
              WHEN (EXPIRY_DATE IS NULL)
                THEN NULL
              WHEN (
                EXPIRY_DATE = CAST((
                  NOT(
                    CASE
                      WHEN ((REGEXP_CONTAINS(EXPIRY_DATE, '^[0-9]+$')) OR ((LENGTH(EXPIRY_DATE)) = 0))
                        THEN TRUE
                      ELSE FALSE
                    END)
                ) AS STRING)
              )
                THEN NULL
              ELSE (
                DATE_ADD(
                  (PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(SUBSTRING(EXPIRY_DATE, 0, 4), '0101')) AS STRING))), 
                  INTERVAL (CAST(SUBSTRING(EXPIRY_DATE, 4, 3) AS FLOAT64) - 1) DAY)
              )
            END
          )
        ELSE NULL
      END
    ) AS O_EXPIRY_DATE,
    (
      CASE
        WHEN (PRIOR_CARD_EXPIRY <> '0000000')
          THEN (
            CASE
              WHEN (PRIOR_CARD_EXPIRY IS NULL)
                THEN NULL
              WHEN (
                PRIOR_CARD_EXPIRY = CAST((
                  NOT(
                    CASE
                      WHEN ((REGEXP_CONTAINS(PRIOR_CARD_EXPIRY, '^[0-9]+$')) OR ((LENGTH(PRIOR_CARD_EXPIRY)) = 0))
                        THEN TRUE
                      ELSE FALSE
                    END)
                ) AS STRING)
              )
                THEN NULL
              ELSE (
                DATE_ADD(
                  (PARSE_TIMESTAMP('%Y%m%d', CAST((CONCAT(SUBSTRING(PRIOR_CARD_EXPIRY, 0, 4), '0101')) AS STRING))), 
                  INTERVAL (CAST(SUBSTRING(PRIOR_CARD_EXPIRY, 4, 3) AS FLOAT64) - 1) DAY)
              )
            END
          )
        ELSE NULL
      END
    ) AS O_PRIOR_CARD_EXPIRY,
    (
      CASE
        WHEN (NUMBER_CARDS IS NOT NULL)
          THEN CAST(NUMBER_CARDS AS FLOAT64)
        ELSE NULL
      END
    ) AS O_NUMBER_CARDS,
    (
      CASE
        WHEN (
          (
            (
              CASE
                WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                  THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
                ELSE CAST(NULL AS STRING)
              END
            ) IS NULL
          )
          OR (
               (
                 LENGTH(
                   CAST((
                     CASE
                       WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                         THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
                       ELSE CAST(NULL AS STRING)
                     END
                   ) AS STRING))
               ) = 0
             )
        )
          THEN (ERROR('No Business date found on DBATTRIBUTE'))
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
    ) AS BUSINESS_DATE,
    (
      DATE_ADD(
        (
          CASE
            WHEN (
              (
                (
                  CASE
                    WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                      THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
                    ELSE CAST(NULL AS STRING)
                  END
                ) IS NULL
              )
              OR (
                   (
                     LENGTH(
                       CAST((
                         CASE
                           WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                             THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
                           ELSE CAST(NULL AS STRING)
                         END
                       ) AS STRING))
                   ) = 0
                 )
            )
              THEN (ERROR('No Business date found on DBATTRIBUTE'))
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
    ) AS BUSINESS_DATE_MINUS_1,
    (UNKNOWN_EXPRESSION('if (over(sum(1), partitionBy(1), orderBy(1)) == 1) output_sp else (real(8))(string)NULL')) AS FEED_UPDATE_ID,
    prophecy_sk AS prophecy_sk,
    PREVIOUS_PROGRAM_ID AS PREVIOUS_PROGRAM_ID,
    ACTIVE_DATE AS ACTIVE_DATE,
    EXPIRY_DATE AS EXPIRY_DATE,
    NUMBER_CARDS AS NUMBER_CARDS,
    EMBOSSER_NAME1 AS EMBOSSER_NAME1,
    PRIOR_CARD_EXPIRY AS PRIOR_CARD_EXPIRY,
    ACTUAL_PROGRAM_ID AS ACTUAL_PROGRAM_ID,
    BUSINESS_DATE_STRING__find_business_date_lkp_1 AS BUSINESS_DATE_STRING__find_business_date_lkp_1,
    LOOKUP_VARIABLE_4 AS LOOKUP_VARIABLE_4,
    output_sp AS output_sp
  
  FROM EXP_CONSTRUCT_CARD_DETAIL_sp AS in0

),

EXP_CONSTRUCT_CARD_DETAIL_EXPR_121 AS (

  SELECT 
    ACCID AS ACCID_IN,
    CARD_NUMBER AS CARD_NUMBER_IN,
    prophecy_sk AS prophecy_sk,
    BUSINESS_DATE_STRING__find_business_date_lkp_1 AS BUSINESS_DATE_STRING__find_business_date_lkp_1,
    BLOCK_CODE AS BLOCK_CODE,
    CARDHOLDER_FLAG AS CARDHOLDER_FLAG,
    ACTV_RQD_CURR AS ACTV_RQD_CURR,
    ACTV_RQD_PREV AS ACTV_RQD_PREV,
    O_EMBOSSER_NAME AS O_EMBOSSER_NAME,
    PRY_SDY_IND AS PRY_SDY_IND,
    CURR_PREV_IND AS CURR_PREV_IND,
    O_ACTUAL_PROGRAM_ID AS O_ACTUAL_PROGRAM_ID,
    O_PREVIOUS_PROGRAM_ID AS O_PREVIOUS_PROGRAM_ID,
    O_ACTIVE_DATE AS O_ACTIVE_DATE,
    O_EXPIRY_DATE AS O_EXPIRY_DATE,
    O_PRIOR_CARD_EXPIRY AS O_PRIOR_CARD_EXPIRY,
    O_NUMBER_CARDS AS O_NUMBER_CARDS,
    BUSINESS_DATE AS BUSINESS_DATE,
    BUSINESS_DATE_MINUS_1 AS BUSINESS_DATE_MINUS_1,
    FEED_UPDATE_ID AS FEED_UPDATE_ID
  
  FROM EXP_CONSTRUCT_CARD_DETAIL_reformat AS in0

),

FIL_CREDIT_DETAIL_JOIN_merged AS (

  SELECT 
    in1.ACCID AS ACCID,
    in1.EFROM AS EFROM,
    in1.CARD_NUMBER AS CARD_NUMBER,
    in0.ACCID_IN AS ACCID_IN,
    in0.prophecy_sk AS prophecy_sk,
    in2.BUSINESS_DATE AS BUSINESS_DATE,
    in0.CARD_NUMBER_IN AS CARD_NUMBER_IN
  
  FROM EXP_CONSTRUCT_CARD_DETAIL_EXPR_121 AS in0
  INNER JOIN SOURCE_LKP_ACCID_ON_CARD_DETAIL AS in1
     ON ((in1.ACCID = in0.ACCID_IN) AND (in1.CARD_NUMBER = in0.CARD_NUMBER_IN))
  INNER JOIN EXP_CONSTRUCT_CARD_DETAIL_reformat AS in2
     ON in0.prophecy_sk = in2.prophecy_sk

),

FIL_CREDIT_DETAIL_JOIN_EXPR_118 AS (

  SELECT 
    ACCID AS ACCID,
    EFROM AS EFROM,
    prophecy_sk AS prophecy_sk,
    CARD_NUMBER AS CARD_NUMBER_IN,
    BUSINESS_DATE AS BUSINESS_DATE
  
  FROM FIL_CREDIT_DETAIL_JOIN_merged AS in0

),

FIL_CREDIT_DETAIL AS (

  SELECT * 
  
  FROM FIL_CREDIT_DETAIL_JOIN_EXPR_118 AS in0
  
  WHERE CAST((ACCID IS NOT NULL) AS BOOL)

),

CLOSE_CARD_DETAIL_EXP AS (

  SELECT 
    ACCID AS ACCID,
    CARD_NUMBER_IN AS CARD_NUMBER,
    EFROM AS EFROM,
    BUSINESS_DATE AS ETO,
    CAST(NULL AS STRING) AS ACTIVE_DATE,
    CAST(NULL AS STRING) AS BLOCK_CODE,
    CAST(NULL AS STRING) AS EXPIRY_DATE,
    CAST(NULL AS STRING) AS CARDHOLDER_FLAG,
    CAST(NULL AS NUMERIC) AS NUMBER_CARDS_ISSUED,
    CAST(NULL AS STRING) AS ACTIVATION_REQ,
    CAST(NULL AS STRING) AS ACTIVATION_RQD_PREV,
    CAST(NULL AS STRING) AS EMBOSSER_NAME,
    CAST(NULL AS STRING) AS PRIOR_CARD_EXPIRY,
    CAST(NULL AS STRING) AS PRY_SDY_IND,
    CAST(NULL AS STRING) AS CURR_PREV_IND,
    CAST(NULL AS NUMERIC) AS ACTUAL_PROGRAM_ID,
    CAST(NULL AS NUMERIC) AS PREVIOUS_PROGRAM_ID,
    CAST(NULL AS STRING) AS CARD_TECHNOLOGY
  
  FROM FIL_CREDIT_DETAIL AS in0

)

SELECT *

FROM CLOSE_CARD_DETAIL_EXP
