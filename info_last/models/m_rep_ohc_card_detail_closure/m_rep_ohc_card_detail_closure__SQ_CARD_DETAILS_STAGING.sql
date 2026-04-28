{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SQ_CARD_DETAILS_STAGING AS (

  SELECT 
    /*+ parallel(F) parallel(AA) use_hash (F AA) ordered*/
    F.CARD_NUMBER,
    F.ACCOUNT_NUMBER,
    F.ACTIVE_DATE,
    F.BLOCK_CODE,
    F.EXPIRY_DATE,
    F.CARDHOLDER_FLAG,
    F.NUMBER_CARDS,
    F.ACTIV_RQD_CURR,
    F.ACTIV_RQD_PREV,
    F.EMBOSSER_NAME1,
    F.PRIOR_CARD_EXPIRY,
    F.PRY_SDY_IND,
    F.CURR_PREV_IND,
    F.ACTUAL_PROGRAM_ID,
    F.PREVIOUS_PROGRAM_ID,
    AA.ACCID
  
  FROM FSK2CARD_STAGING AS F, ACCOUNT_TRANSFER_HISTORY AS AA
  
  WHERE F.ACCOUNT_NUMBER = AA.PREV_ACCOUNT_ALIAS_ID AND F.TRANSFER_INDICATOR = 'O'

)

SELECT *

FROM SQ_CARD_DETAILS_STAGING
