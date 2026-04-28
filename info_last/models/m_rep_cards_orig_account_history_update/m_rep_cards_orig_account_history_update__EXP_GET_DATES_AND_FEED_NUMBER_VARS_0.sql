{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ ref('s1')}}

),

SQ_WORK_CARDS_ORIG_STAGING AS (

  SELECT *
  
  FROM (
    SELECT 
      /*+  parallel(wrk)  parallel(account_history) parallel(account_alias) use_hash(wrk account_history account_alias)*/
      account_alias.accid accid_account_alias,
      wrk.application_store_number AS application_store_number_src,
      CASE
        WHEN wrk.application_status = 'CANCELLED'
          THEN 'CA'
        WHEN wrk.application_status = 'APPROVED'
          THEN 'AP'
        WHEN wrk.application_status = 'ABORTED'
          THEN 'AX'
        WHEN wrk.application_status = 'PRODUCT_ONBOARDING_COMPLETED'
          THEN 'AF'
        WHEN wrk.application_status = 'REFERRED'
          THEN 'RF'
        WHEN wrk.application_status = 'DECLINED'
          THEN 'DE'
      END AS status_src,
      wrk.last_action_date AS status_date_src,
      account_history.accid AS accid_tgt,
      account_history.domicile_branch AS domicile_branch_tgt,
      account_history.status AS status_tgt,
      account_history.status_date AS status_date_tgt,
      account_history.efrom,
      row_number() OVER (PARTITION BY wrk.application_number ORDER BY wrk.last_action_date DESC) row_number
    
    FROM work_cards_orig_staging AS wrk, account_history AS partition(hot), account_alias
    
    WHERE wrk.application_number = account_alias.account_alias_id
          and account_alias.accid = account_history.accid (+)
          and account_alias.source_system_code = 'U'
          and account_alias.class = '563'
          and account_alias.eto IS NULL
          and account_history.eto (+) IS NULL
  )
  
  WHERE row_number = 1

),

EXP_COMPARE_ACCOUNT_HISTORY AS (

  SELECT 
    ACCID_ACCOUNT_ALIAS AS ACCID_ACCOUNT_ALIAS,
    ACCID AS ACCID_TGT,
    APPLICATION_STORE_NUMBER AS APPLICATION_STORE_NUMBER_SRC,
    LAST_ACTION_DATE AS STATUS_DATE_SRC,
    APPLICATION_STATUS AS STATUS_SRC,
    DOMICILE_BRANCH AS DOMICILE_BRANCH_TGT,
    STATUS AS STATUS_TGT,
    STATUS_DATE AS STATUS_DATE_TGT,
    EFROM AS EFROM_TGT,
    (
      (
        (
          CASE
            WHEN (
              (
                (((LENGTH(APPLICATION_STORE_NUMBER)) = 0) OR (APPLICATION_STORE_NUMBER IS NULL))
                AND (DOMICILE_BRANCH IS NULL)
              )
              OR (APPLICATION_STORE_NUMBER = DOMICILE_BRANCH)
            )
              THEN 0
            ELSE 1
          END
        )
        + (
            CASE
              WHEN (APPLICATION_STATUS = STATUS)
                THEN 0
              ELSE 1
            END
          )
      )
      + (
          CASE
            WHEN (LAST_ACTION_DATE = STATUS_DATE)
              THEN 0
            ELSE 1
          END
        )
    ) AS total_compare
  
  FROM SQ_WORK_CARDS_ORIG_STAGING AS in0

),

EXP_GET_DATES_AND_FEED_NUMBER_LOOKUP_13 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_3,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.ACCID_ACCOUNT_ALIAS AS ACCID_ACCOUNT_ALIAS,
    in1.ACCID_TGT AS ACCID_TGT,
    in1.STATUS_DATE_SRC AS STATUS_DATE_SRC,
    in1.STATUS_SRC AS STATUS_SRC,
    in1.DOMICILE_BRANCH_TGT AS DOMICILE_BRANCH_TGT,
    in1.STATUS_TGT AS STATUS_TGT,
    in1.STATUS_DATE_TGT AS STATUS_DATE_TGT,
    in1.total_compare AS total_compare,
    in1.APPLICATION_STORE_NUMBER_SRC AS APPLICATION_STORE_NUMBER_SRC,
    in1.EFROM_TGT AS EFROM_TGT
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN EXP_COMPARE_ACCOUNT_HISTORY AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_GET_DATES_AND_FEED_NUMBER_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_3 AS business_date_string__find_business_date_lkp_1,
    *
  
  FROM EXP_GET_DATES_AND_FEED_NUMBER_LOOKUP_13 AS in0

)

SELECT *

FROM EXP_GET_DATES_AND_FEED_NUMBER_VARS_0
