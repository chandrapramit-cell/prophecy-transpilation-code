{{
  config({    
    "materialized": "table",
    "alias": "REP_G2_ACCOUNT_TFR_STAGING",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH account_transfer AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_ohc_account_transfer_staging', 'account_transfer') }}

),

SQ_account_transfer_EXPR_106 AS (

  SELECT 
    old_acct AS OLD_ACCOUNT_NUMBER,
    new_acct AS NEW_ACCOUNT_NUMBER,
    tfr_eff_date AS TRANSFER_EFFECTIVE_DATE
  
  FROM account_transfer AS in0

)

SELECT *

FROM SQ_account_transfer_EXPR_106
