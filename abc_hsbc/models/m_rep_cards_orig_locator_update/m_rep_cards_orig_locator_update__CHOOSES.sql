{{
  config({    
    "materialized": "table",
    "alias": "CHOOSES",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CARDS_ORIG_LOCATOR_reformat AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_locator_update__EXP_CARDS_ORIG_LOCATOR_reformat')}}

),

RTR_CARDS_ORIG_LOCATOR_out3 AS (

  SELECT * 
  
  FROM EXP_CARDS_ORIG_LOCATOR_reformat AS in0
  
  WHERE (
          (
            ((CLID_LOCATOR IS NULL) AND (VALUE IS NOT NULL))
            OR (((CLID_LOCATOR IS NOT NULL) AND (VALUE IS NOT NULL)) AND (TOTAL_COMPARE <> 0))
          )
          OR ((CLID_LOCATOR IS NOT NULL) AND (VALUE IS NULL))
        )

),

RTR_CARDS_ORIG_LOCATOR_EXPR_NEW_CHOOSES AS (

  SELECT 
    CLID_CLIENT_ALIAS AS CLID,
    business_date AS CLIENT_HISTORY_EFFECT_FROM,
    feed_update_id AS FEED_UPDATE_ID
  
  FROM RTR_CARDS_ORIG_LOCATOR_out3 AS in0

)

SELECT *

FROM RTR_CARDS_ORIG_LOCATOR_EXPR_NEW_CHOOSES
