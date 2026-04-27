{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_ACCOUNT_ALIAS AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_account_alias_update', 'EXP_ACCOUNT_ALIAS') }}

),

RTR_FORTRAV_ACCOUNT_ALIAS AS (

  SELECT * 
  
  FROM EXP_ACCOUNT_ALIAS AS in0
  
  WHERE ((NEW_ACCID IS NOT NULL) AND (EXISTING_ACCID IS NULL))

),

RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS AS (

  SELECT 
    APPLICATION_NUMBER AS APPLICATION_NUMBER1,
    NEW_ACCID AS NEW_ACCID1,
    EXISTING_ACCID AS EXISTING_ACCID1,
    SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE1,
    CLASS AS CLASS1,
    business_date AS business_date1,
    feed_update_id AS feed_update_id1,
    NEW_ACCID AS ACCID,
    SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE,
    APPLICATION_NUMBER AS ACCOUNT_ALIAS_ID,
    CLASS AS CLASS,
    business_date AS EFROM,
    feed_update_id AS FEED_UPDATE_ID
  
  FROM RTR_FORTRAV_ACCOUNT_ALIAS AS in0

)

SELECT *

FROM RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS
