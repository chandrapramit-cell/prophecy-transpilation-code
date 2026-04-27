{{
  config({    
    "materialized": "table",
    "alias": "NEW_MAINTAINS",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1 AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1')}}

),

RTR_ACCOUNT_HISTORY_CHANGE_out3 AS (

  SELECT * 
  
  FROM EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1 AS in0
  
  WHERE ((ACCID_TGT IS NULL) OR ((ACCID_TGT IS NOT NULL) AND (total_compare <> 0)))

),

RTR_ACCOUNT_HISTORY_CHANGE_EXPR_MAINTAINS AS (

  SELECT 
    ACCID_ACCOUNT_ALIAS AS ACCID,
    BUSINESS_DATE AS ACCOUNT_HISTORY_EFFECT_FROM,
    feed_update_id AS FEED_UPDATE_ID
  
  FROM RTR_ACCOUNT_HISTORY_CHANGE_out3 AS in0

)

SELECT *

FROM RTR_ACCOUNT_HISTORY_CHANGE_EXPR_MAINTAINS
