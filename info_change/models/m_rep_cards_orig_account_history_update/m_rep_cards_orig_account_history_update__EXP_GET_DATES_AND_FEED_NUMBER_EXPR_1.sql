{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_GET_DATES_AND_FEED_NUMBER AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_account_history_update', 'EXP_GET_DATES_AND_FEED_NUMBER') }}

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
  
  FROM EXP_GET_DATES_AND_FEED_NUMBER AS in0

)

SELECT *

FROM EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1
