{{
  config({    
    "materialized": "table",
    "alias": "CHOOSES",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS')}}

),

CHOOSES_EXP AS (

  SELECT 
    CLID AS CLID,
    CLIENT_HISTORY_EFFECT_FROM AS CLIENT_HISTORY_EFFECT_FROM,
    FEED_UPDATE_ID AS FEED_UPDATE_ID
  
  FROM RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS AS in0

)

SELECT *

FROM CHOOSES_EXP
