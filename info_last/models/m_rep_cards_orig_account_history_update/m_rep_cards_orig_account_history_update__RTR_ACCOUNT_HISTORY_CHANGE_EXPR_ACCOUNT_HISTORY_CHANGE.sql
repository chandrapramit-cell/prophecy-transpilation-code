{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1 AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_account_history_update__EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1')}}

),

RTR_ACCOUNT_HISTORY_CHANGE_out0 AS (

  SELECT * 
  
  FROM EXP_GET_DATES_AND_FEED_NUMBER_EXPR_1 AS in0
  
  WHERE (
          (
            (ACCID_TGT IS NOT NULL)
            AND (
                  (
                    CASE
                      WHEN ((DATE_DIFF(CAST(EFROM_TGT AS DATE), CAST(BUSINESS_DATE AS DATE), DAY)) > 0)
                        THEN 1
                      WHEN ((DATE_DIFF(CAST(EFROM_TGT AS DATE), CAST(BUSINESS_DATE AS DATE), DAY)) < 0)
                        THEN -1
                      ELSE (DATE_DIFF(CAST(EFROM_TGT AS DATE), CAST(BUSINESS_DATE AS DATE), DAY))
                    END
                  ) <> 0
                )
          )
          AND (total_compare <> 0)
        )

),

RTR_ACCOUNT_HISTORY_CHANGE_EXPR_ACCOUNT_HISTORY_CHANGE AS (

  SELECT 
    ACCID_ACCOUNT_ALIAS AS ACCID_ACCOUNT_ALIAS1,
    ACCID_TGT AS ACCID_TGT1,
    EFROM_TGT AS EFROM_TGT1,
    APPLICATION_STORE_NUMBER_SRC AS APPLICATION_STORE_NUMBER_SRC1,
    STATUS_SRC AS STATUS_SRC1,
    STATUS_DATE_SRC AS STATUS_DATE_SRC1,
    total_compare AS total_compare1,
    BUSINESS_DATE AS BUSINESS_DATE1,
    BUSINESS_DATE_MINUS_1 AS BUSINESS_DATE_MINUS_11,
    feed_update_id AS feed_update_id1,
    ACCID_TGT AS ACCID,
    EFROM_TGT AS EFROM,
    BUSINESS_DATE_MINUS_1 AS ETO
  
  FROM RTR_ACCOUNT_HISTORY_CHANGE_out0 AS in0

)

SELECT *

FROM RTR_ACCOUNT_HISTORY_CHANGE_EXPR_ACCOUNT_HISTORY_CHANGE
