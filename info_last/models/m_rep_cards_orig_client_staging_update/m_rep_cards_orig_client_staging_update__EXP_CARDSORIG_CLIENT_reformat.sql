{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CARDSORIG_CLIENT_sp AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_client_staging_update', 'EXP_CARDSORIG_CLIENT_sp') }}

),

EXP_CARDSORIG_CLIENT_reformat AS (

  SELECT 
    CLID AS CLID,
    MERGE_CLID AS MERGE_CLID,
    EFROM AS EFROM,
    GENDER AS GENDER,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    CLID_LKP AS CLID_LKP,
    TITLE_LKP AS TITLE_LKP,
    SURNAME_LKP AS SURNAME_LKP,
    TITLE AS TITLE,
    INITIALS AS INITIALS,
    FORENAMES AS FORENAMES,
    SURNAME AS SURNAME,
    DOB AS DOB,
    150 AS CLIENT_CLASS,
    0 AS SOURCE_TYPE,
    'U' AS SOURCE_SYSTEM_CODE,
    1 AS SOURCE_RECORD_TYPE,
    1 AS CLIENT_STATUS,
    (
      CASE
        WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1)
          THEN output_sp
        ELSE NULL
      END
    ) AS feed_update_id,
    (
      CASE
        WHEN CAST((
          (
            CASE
              WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
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
                    THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
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
                    THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
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
                        THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
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
      (
        (
          (
            (
              CASE
                WHEN (TITLE_LKP = TITLE)
                  THEN 0
                ELSE 1
              END
            )
            + (
                CASE
                  WHEN (FORENAMES_LKP = FORENAMES)
                    THEN 0
                  ELSE 1
                END
              )
          )
          + (
              CASE
                WHEN (SURNAME_LKP = SURNAME)
                  THEN 0
                ELSE 1
              END
            )
        )
        + (
            CASE
              WHEN (DOB_LKP = DOB)
                THEN 0
              ELSE 1
            END
          )
      )
      + (
          CASE
            WHEN (GENDER_LKP = GENDER)
              THEN 0
            ELSE 1
          END
        )
    ) AS TOTAL_COMPARE,
    (
      CASE
        WHEN (
          (
            CASE
              WHEN (
                DATE_DIFF(
                  CAST((
                    CASE
                      WHEN CAST((
                        (
                          CASE
                            WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                              THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
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
                                  THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
                                ELSE CAST(NULL AS STRING)
                              END
                            ), 
                            0, 
                            8) AS STRING))
                      )
                    END
                  ) AS DATE), 
                  CAST(EFROM_LKP AS DATE), 
                  DAY) > 0
              )
                THEN 1
              WHEN (
                DATE_DIFF(
                  CAST((
                    CASE
                      WHEN CAST((
                        (
                          CASE
                            WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                              THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
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
                                  THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
                                ELSE CAST(NULL AS STRING)
                              END
                            ), 
                            0, 
                            8) AS STRING))
                      )
                    END
                  ) AS DATE), 
                  CAST(EFROM_LKP AS DATE), 
                  DAY) < 0
              )
                THEN -1
              ELSE DATE_DIFF(
                CAST((
                  CASE
                    WHEN CAST((
                      (
                        CASE
                          WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                            THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
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
                                THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_3
                              ELSE CAST(NULL AS STRING)
                            END
                          ), 
                          0, 
                          8) AS STRING))
                    )
                  END
                ) AS DATE), 
                CAST(EFROM_LKP AS DATE), 
                DAY)
            END
          ) = 0
        )
          THEN 0
        ELSE 1
      END
    ) AS BUSINESS_DATE_CHK,
    prophecy_sk AS prophecy_sk,
    CLIENT_STATUS_LKP AS CLIENT_STATUS_LKP,
    MERGE_CLID_LKP AS MERGE_CLID_LKP,
    EFROM_LKP AS EFROM_LKP,
    FORENAMES_LKP AS FORENAMES_LKP,
    GENDER_LKP AS GENDER_LKP,
    DOB_LKP AS DOB_LKP,
    lookup_string AS lookup_string,
    business_date_string__find_business_date_lkp_3 AS business_date_string__find_business_date_lkp_3,
    LOOKUP_VARIABLE_8 AS LOOKUP_VARIABLE_8,
    output_sp AS output_sp
  
  FROM EXP_CARDSORIG_CLIENT_sp AS in0

)

SELECT *

FROM EXP_CARDSORIG_CLIENT_reformat
