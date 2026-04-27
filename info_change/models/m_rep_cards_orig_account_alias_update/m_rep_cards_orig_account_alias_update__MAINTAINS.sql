{{
  config({    
    "materialized": "table",
    "alias": "MAINTAINS",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_account_alias_update__RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS')}}

),

RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS_EXPR_16 AS (

  SELECT 
    NEW_ACCID1 AS ACCID,
    business_date1 AS ACCOUNT_HISTORY_EFFECT_FROM,
    feed_update_id1 AS FEED_UPDATE_ID
  
  FROM RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS AS in0

)

SELECT *

FROM RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS_EXPR_16
