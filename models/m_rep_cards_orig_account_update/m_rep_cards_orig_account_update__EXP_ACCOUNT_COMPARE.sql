{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SQ_WORK_CARDS_ORIG_STAGING AS (

  SELECT /*+ parallel(wrk) parallel(aa) parallel(a) parallel(ca)  use_hash(wrk aa a ca)*/
         *
  
  FROM (
    SELECT 
      aa.accid AS account_alias_accid,
      a.accid AS account_accid,
      ca.clid,
      ca.merge_clid,
      wrk.application_number,
      wrk.product,
      wrk.application_source_code,
      wrk.application_status,
      wrk.last_action_date,
      wrk.application_store_number,
      a.acc_type,
      a.source_code,
      a.status,
      a.status_date,
      a.date_last_acc_status_change,
      a.domicile_branch,
      row_number() OVER (PARTITION BY wrk.application_number ORDER BY wrk.last_action_date DESC) row_number
    
    FROM work_cards_orig_staging AS wrk, account_alias AS aa, account AS a, client_alias AS ca
    
    WHERE wrk.application_number = aa.account_alias_id (+)
          and aa.source_system_code (+) = 'U'
          and aa.eto (+) IS NULL
          and aa.accid = a.accid (+)
          and wrk.customer_number = ca.source_client_ref (+)
          and ca.source_system_code (+) = 'U'
          and ca.source_type (+) = 0
          and ca.class (+) = '585'
          and ca.eto (+) IS NULL
  )
  
  WHERE row_number = 1

),

EXP_ACCOUNT_COMPARE AS (

  SELECT 
    ACCID_ACCOUNT_ALIAS AS ACCID_ACCOUNT_ALIAS,
    ACCID_ACCOUNT AS ACCID_ACCOUNT,
    CLID_CLIENT_ALIAS AS CLID_CLIENT_ALIAS,
    MERGE_CLID AS MERGE_CLID,
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    (LTRIM((RTRIM(SUBSTRING(PRODUCT, 0, 2))))) AS o_PRODUCT_SRC,
    APPLICATION_SOURCE_CODE AS APPLICATION_SOURCE_CODE,
    APPLICATION_STORE_NUMBER AS APPLICATION_STORE_NUMBER,
    (
      CASE
        WHEN (APPLICATION_STATUS = 'CANCELLED')
          THEN 'CA'
        WHEN (APPLICATION_STATUS = 'APPROVED')
          THEN 'AP'
        WHEN (APPLICATION_STATUS = 'ABORTED')
          THEN 'AX'
        WHEN (APPLICATION_STATUS = 'PRODUCT_ONBOARDING_COMPLETED')
          THEN 'AF'
        WHEN (APPLICATION_STATUS = 'REFERRED')
          THEN 'RF'
        WHEN (APPLICATION_STATUS = 'DECLINED')
          THEN 'DE'
        ELSE NULL
      END
    ) AS o_APPLICATION_STATUS_SRC,
    LAST_ACTION_DATE AS LAST_ACTION_DATE_SRC,
    ACC_TYPE AS ACC_TYPE_TGT,
    SOURCE_CODE AS SOURCE_CODE_TGT,
    DOMICILE_BRANCH AS DOMICILE_BRANCH_TGT,
    STATUS AS STATUS_TGT,
    STATUS_DATE AS STATUS_DATE_TGT,
    DATE_LAST_ACC_STATUS_CHANGE AS DATE_LAST_ACC_STATUS_CHANGE_TGT,
    (
      CASE
        WHEN (
          (
            (((LENGTH(CAST(SUBSTRING(PRODUCT, 0, 2) AS STRING))) = 0) OR (SUBSTRING(PRODUCT, 0, 2) IS NULL))
            AND (ACC_TYPE IS NULL)
          )
          OR (SUBSTRING(PRODUCT, 0, 2) = ACC_TYPE)
        )
          THEN 0
        ELSE 1
      END
    ) AS o_comp_acc_type,
    (
      CASE
        WHEN (
          (
            (((LENGTH(APPLICATION_SOURCE_CODE)) = 0) OR (APPLICATION_SOURCE_CODE IS NULL))
            AND (SOURCE_CODE IS NULL)
          )
          OR (APPLICATION_SOURCE_CODE = SOURCE_CODE)
        )
          THEN 0
        ELSE 1
      END
    ) AS o_comp_source_code,
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
    ) AS o_comp_store_number,
    (
      CASE
        WHEN (
          CAST((
            CASE
              WHEN (APPLICATION_STATUS = 'CANCELLED')
                THEN 'CA'
              WHEN (APPLICATION_STATUS = 'APPROVED')
                THEN 'AP'
              WHEN (APPLICATION_STATUS = 'ABORTED')
                THEN 'AX'
              WHEN (APPLICATION_STATUS = 'PRODUCT_ONBOARDING_COMPLETED')
                THEN 'AF'
              WHEN (APPLICATION_STATUS = 'REFERRED')
                THEN 'RF'
              WHEN (APPLICATION_STATUS = 'DECLINED')
                THEN 'DE'
              ELSE NULL
            END
          ) AS STRING) = STATUS
        )
          THEN 0
        ELSE 1
      END
    ) AS o_compare_status,
    (
      CASE
        WHEN (STATUS_DATE = LAST_ACTION_DATE)
          THEN 0
        ELSE 1
      END
    ) AS o_compare_status_date,
    (ROW_NUMBER() OVER ()) AS prophecy_sk
  
  FROM SQ_WORK_CARDS_ORIG_STAGING AS in0

)

SELECT *

FROM EXP_ACCOUNT_COMPARE
