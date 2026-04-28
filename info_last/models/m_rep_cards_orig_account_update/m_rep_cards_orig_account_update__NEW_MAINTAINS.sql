{{
  config({    
    "materialized": "table",
    "alias": "NEW_MAINTAINS",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTR_ACCOUNT_JOIN_EXPR_42 AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_account_update__RTR_ACCOUNT_JOIN_EXPR_42')}}

),

RTR_ACCOUNT_out3 AS (

  SELECT * 
  
  FROM RTR_ACCOUNT_JOIN_EXPR_42 AS in0
  
  WHERE ((ACCID_ACCOUNT IS NULL) OR ((ACCID_ACCOUNT IS NOT NULL) AND (TOTAL_COMPARE <> 0)))

),

RTR_ACCOUNT_EXPR_MAINTAINS AS (

  SELECT 
    ACCID_ACCOUNT_ALIAS AS ACCID,
    BUSINESS_DATE AS ACCOUNT_HISTORY_EFFECT_FROM,
    feed_update_id AS FEED_UPDATE_ID
  
  FROM RTR_ACCOUNT_out3 AS in0

)

SELECT *

FROM RTR_ACCOUNT_EXPR_MAINTAINS
