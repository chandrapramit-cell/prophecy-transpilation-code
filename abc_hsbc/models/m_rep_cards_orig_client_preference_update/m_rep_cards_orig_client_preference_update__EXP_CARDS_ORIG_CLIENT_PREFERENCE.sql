{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'DBATTRIBUTE') }}

),

SQ_CLIENT_PREFERENCE AS (

  SELECT 
    /*+ parallel(cus) parallel(ca) parallel(cp) use_hash(cus ca cp)*/
    DISTINCT ca.clid AS client_alias_clid,
    cp.clid AS client_preference_clid,
    CASE
      WHEN MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'CALL'
        THEN 'Y'
      ELSE 'N'
    END AS marketing_opt_in_tel,
    CASE
      WHEN MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'EMAIL'
        THEN 'Y'
      ELSE 'N'
    END AS marketing_opt_in_email,
    CASE
      WHEN MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'SMS'
        THEN 'Y'
      ELSE 'N'
    END AS marketing_opt_in_text,
    CASE
      WHEN MARKETING_PREFERENCES_PREFERRED_CHANNELS = 'POST'
        THEN 'Y'
      ELSE 'N'
    END AS marketing_opt_in_post,
    cp.mail_preference,
    cp.mail_preference_date,
    cp.phone_preference,
    cp.phone_preference_date,
    cp.email_preference,
    cp.email_preference_date,
    cp.sms_preference,
    cp.sms_preference_date,
    row_number() OVER (PARTITION BY cus.customer_number ORDER BY cus.last_action_date DESC) row_number
  
  FROM work_cards_orig_staging AS cus, client_alias AS ca, client_preference AS cp
  
  WHERE cus.customer_number = ca.source_client_ref (+)
        and cus.customer_number IS NOT NULL
        and ca.source_type (+) = 0
        and ca.source_system_code (+) = 'U'
        and ca.eto (+) IS NULL
        and ca.clid = cp.clid (+)
        and ca.class (+) = '585'
        and cp.source_system_code (+) = 'U'

),

EXP_CARDS_ORIG_CLIENT_PREFERENCE_LOOKUP_68 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_2,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.CLID_CLIENT_PREFERENCE AS CLID_CLIENT_PREFERENCE,
    in1.MARKETING_OPT_IN_TEL AS MARKETING_OPT_IN_TEL,
    in1.MARKETING_OPT_IN_EMAIL AS MARKETING_OPT_IN_EMAIL,
    in1.PHONE_PREFERENCE_DATE AS PHONE_PREFERENCE_DATE,
    in1.EMAIL_PREFERENCE AS EMAIL_PREFERENCE,
    in1.EMAIL_PREFERENCE_DATE AS EMAIL_PREFERENCE_DATE,
    in1.MAIL_PREFERENCE_DATE AS MAIL_PREFERENCE_DATE,
    in1.PHONE_PREFERENCE AS PHONE_PREFERENCE,
    in1.ROW_NUMBER AS ROW_NUMBER,
    in1.CLID_CLIENT_ALIAS AS CLID_CLIENT_ALIAS,
    in1.SMS_PREFERENCE AS SMS_PREFERENCE,
    in1.SMS_PREFERENCE_DATE AS SMS_PREFERENCE_DATE,
    in1.MARKETING_OPT_IN_TEXT AS MARKETING_OPT_IN_TEXT,
    in1.MARKETING_OPT_IN_POST AS MARKETING_OPT_IN_POST,
    in1.MAIL_PREFERENCE AS MAIL_PREFERENCE
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN SQ_CLIENT_PREFERENCE AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_CARDS_ORIG_CLIENT_PREFERENCE_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_2 AS business_date_string__find_business_date_lkp_1,
    *
  
  FROM EXP_CARDS_ORIG_CLIENT_PREFERENCE_LOOKUP_68 AS in0

),

EXP_CARDS_ORIG_CLIENT_PREFERENCE AS (

  SELECT 
    CLID_CLIENT_ALIAS AS CLID_CLIENT_ALIAS,
    CLID_CLIENT_PREFERENCE AS CLID_CLIENT_PREFERENCE,
    MARKETING_OPT_IN_TEL AS MARKETING_OPT_IN_TEL,
    MARKETING_OPT_IN_EMAIL AS MARKETING_OPT_IN_EMAIL,
    MARKETING_OPT_IN_TEXT AS MARKETING_OPT_IN_TEXT,
    MARKETING_OPT_IN_POST AS MARKETING_OPT_IN_POST,
    MAIL_PREFERENCE AS MAIL_PREFERENCE,
    MAIL_PREFERENCE_DATE AS MAIL_PREFERENCE_DATE,
    PHONE_PREFERENCE AS PHONE_PREFERENCE,
    PHONE_PREFERENCE_DATE AS PHONE_PREFERENCE_DATE,
    EMAIL_PREFERENCE AS EMAIL_PREFERENCE,
    EMAIL_PREFERENCE_DATE AS EMAIL_PREFERENCE_DATE,
    SMS_PREFERENCE AS SMS_PREFERENCE,
    SMS_PREFERENCE_DATE AS SMS_PREFERENCE_DATE,
    'U' AS SOURCE_SYSTEM_CODE,
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
    ROW_NUMBER AS ROW_NUMBER,
    (
      (
        (
          (
            CASE
              WHEN (MARKETING_OPT_IN_TEL = PHONE_PREFERENCE)
                THEN 0
              ELSE 1
            END
          )
          + (
              CASE
                WHEN (MARKETING_OPT_IN_EMAIL = EMAIL_PREFERENCE)
                  THEN 0
                ELSE 1
              END
            )
        )
        + (
            CASE
              WHEN (MARKETING_OPT_IN_POST = MAIL_PREFERENCE)
                THEN 0
              ELSE 1
            END
          )
      )
      + (
          CASE
            WHEN (MARKETING_OPT_IN_TEXT = SMS_PREFERENCE)
              THEN 0
            ELSE 1
          END
        )
    ) AS TOTAL_COMPARE
  
  FROM EXP_CARDS_ORIG_CLIENT_PREFERENCE_VARS_0 AS in0

)

SELECT *

FROM EXP_CARDS_ORIG_CLIENT_PREFERENCE
