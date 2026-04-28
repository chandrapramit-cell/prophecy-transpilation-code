{{
  config({    
    "materialized": "table",
    "alias": "NEW_CHOOSES",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTR_ACCOUNT_JOIN_EXPR_42 AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_account_update__RTR_ACCOUNT_JOIN_EXPR_42')}}

),

RTR_ACCOUNT_out5 AS (

  SELECT * 
  
  FROM RTR_ACCOUNT_JOIN_EXPR_42 AS in0
  
  WHERE ((CLID_CLIENT_ALIAS IS NOT NULL) AND (ACCID_ACCOUNT IS NULL))

),

RTR_ACCOUNT_EXPR_CHOOSES AS (

  SELECT 
    CLID_CLIENT_ALIAS AS CLID,
    BUSINESS_DATE AS CLIENT_HISTORY_EFFECT_FROM,
    feed_update_id AS FEED_UPDATE_ID
  
  FROM RTR_ACCOUNT_out5 AS in0

)

SELECT *

FROM RTR_ACCOUNT_EXPR_CHOOSES
