{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CARDSORIG_CLIENT AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_client_staging_update__EXP_CARDSORIG_CLIENT')}}

),

EXP_CARDSORIG_CLIENT_EXPR_12 AS (

  SELECT 
    CLID AS CLID,
    MERGE_CLID AS MERGE_CLID,
    EFROM AS EFROM,
    CLID_LKP AS CLID_LKP,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    TITLE AS OUTPUT_TITLE,
    INITIALS AS INITIALS,
    FORENAMES AS FORENAMES,
    SURNAME AS SURNAME,
    DOB AS DOB,
    GENDER AS GENDER,
    CLIENT_CLASS AS CLIENT_CLASS,
    SOURCE_TYPE AS SOURCE_TYPE,
    SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE,
    SOURCE_RECORD_TYPE AS SOURCE_RECORD_TYPE,
    CLIENT_STATUS AS CLIENT_STATUS,
    feed_update_id AS feed_update_id,
    business_date AS business_date,
    business_date_minus_1 AS business_date_minus_1,
    TOTAL_COMPARE AS TOTAL_COMPARE,
    TITLE_LKP AS TITLE_LKP,
    SURNAME_LKP AS SURNAME_LKP,
    BUSINESS_DATE_CHK AS BUSINESS_DATE_CHK,
    count1 AS count1,
    lookup_string AS lookup_string,
    businessdate AS businessdate,
    business_date_string AS business_date_string,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_CARDSORIG_CLIENT AS in0

)

SELECT *

FROM EXP_CARDSORIG_CLIENT_EXPR_12
