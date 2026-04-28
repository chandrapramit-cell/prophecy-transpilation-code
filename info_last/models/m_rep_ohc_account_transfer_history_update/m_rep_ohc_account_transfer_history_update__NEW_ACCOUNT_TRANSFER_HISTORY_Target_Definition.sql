{{
  config({    
    "materialized": "table",
    "alias": "NEW_ACCOUNT_TRANSFER_HISTORY_Target_Definition",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTRTRANS_JOIN_EXPR_107 AS (

  SELECT *
  
  FROM {{ ref('m_rep_ohc_account_transfer_history_update__RTRTRANS_JOIN_EXPR_107')}}

),

RTRTRANS_out0 AS (

  SELECT * 
  
  FROM RTRTRANS_JOIN_EXPR_107 AS in0
  
  WHERE (LKP_ACCID IS NULL)

),

NEW_ACCOUNT_TRANSFER_HISTORY_Target_Definition_EXP AS (

  SELECT 
    PRIMARY_ACCID AS ACCID,
    business_date AS EFROM,
    CAST(NULL AS STRING) AS ETO,
    OLD_ACCOUNT_NUMBER AS PREV_ACCOUNT_ALIAS_ID,
    NEW_ACCOUNT_NUMBER AS CURR_ACCOUNT_ALIAS_ID,
    SOURCE_SYSTEM_CODE1 AS SOURCE_SYSTEM_CODE,
    TRANSFER_EFFECTIVE_DATE AS TRANSFER_EFFECTIVE_DATE,
    1 AS NUMBER_OF_TRANSFERS
  
  FROM RTRTRANS_out0 AS in0

)

SELECT *

FROM NEW_ACCOUNT_TRANSFER_HISTORY_Target_Definition_EXP
