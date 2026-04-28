{{
  config({    
    "materialized": "table",
    "alias": "INS_CHOOSES",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CLIENT_FLAG_reformat AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_client_flag_update__EXP_CLIENT_FLAG_reformat')}}

),

RTR_CLIENT_FLAG_out1 AS (

  SELECT * 
  
  FROM EXP_CLIENT_FLAG_reformat AS in0
  
  WHERE (
          (APPLICATION_STATUS = 'DECLINED')
          AND (CAST((DATE_ADD(CAST(business_date AS DATE), INTERVAL -6 MONTH)) AS TIMESTAMP) < LAST_ACTION_DATE)
        )

),

RTR_CLIENT_FLAG_EXPR_INS_CHOOSES AS (

  SELECT 
    CLID AS CLID,
    business_date AS CLIENT_HISTORY_EFFECT_FROM,
    feed_update_id AS FEED_UPDATE_ID
  
  FROM RTR_CLIENT_FLAG_out1 AS in0

)

SELECT *

FROM RTR_CLIENT_FLAG_EXPR_INS_CHOOSES
