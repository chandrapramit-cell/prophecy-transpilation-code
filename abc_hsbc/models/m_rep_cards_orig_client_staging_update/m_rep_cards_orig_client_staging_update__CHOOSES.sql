{{
  config({    
    "materialized": "table",
    "alias": "CHOOSES",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CARDSORIG_CLIENT_EXPR_40 AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT_EXPR_40')}}

),

RTR_CARDSORIG_CLIENT_out3 AS (

  SELECT * 
  
  FROM EXP_CARDSORIG_CLIENT_EXPR_40 AS in0
  
  WHERE (
          (
            (CLID_LKP IS NULL)
            OR (((CLID_LKP IS NOT NULL) AND (TOTAL_COMPARE <> 0)) AND (BUSINESS_DATE_CHK = 0))
          )
          OR (((CLID_LKP IS NOT NULL) AND (TOTAL_COMPARE <> 0)) AND (BUSINESS_DATE_CHK <> 0))
        )

),

RTR_CARDSORIG_CLIENT_EXPR_CHOOSES_INSERT AS (

  SELECT 
    CLID AS CLID,
    feed_update_id AS FEED_UPDATE_ID,
    business_date AS CLIENT_HISTORY_EFFECT_FROM
  
  FROM RTR_CARDSORIG_CLIENT_out3 AS in0

)

SELECT *

FROM RTR_CARDSORIG_CLIENT_EXPR_CHOOSES_INSERT
