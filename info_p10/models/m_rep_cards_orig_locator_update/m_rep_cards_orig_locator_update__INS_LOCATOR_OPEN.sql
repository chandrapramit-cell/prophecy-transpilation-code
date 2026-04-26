{{
  config({    
    "materialized": "table",
    "alias": "INS_LOCATOR_OPEN",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTR_CARDS_ORIG_LOCATOR_EXPR_OPEN_CLOSE_LOCATOR AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_locator_update__RTR_CARDS_ORIG_LOCATOR_EXPR_OPEN_CLOSE_LOCATOR')}}

),

INS_LOCATOR_OPEN1_add_missing_column_0 AS (

  SELECT 
    CAST(NULL AS TIMESTAMP) AS ETO,
    *
  
  FROM RTR_CARDS_ORIG_LOCATOR_EXPR_OPEN_CLOSE_LOCATOR AS in0

),

INS_LOCATOR_OPEN_EXP AS (

  SELECT 
    CLID_CLIENT_ALIAS3 AS CLID,
    SOURCE_TYPE3 AS SOURCE_TYPE,
    MERGE_CLID_CLIENT_ALIAS3 AS MERGE_CLID,
    business_date3 AS EFROM,
    CLASS3 AS CLASS,
    SUBCLASS3 AS SUBCLASS,
    VALUE3 AS `VALUE`,
    CAST(NULL AS STRING) AS CONTACT_POINT_EXTRA_INFO,
    ETO AS ETO
  
  FROM INS_LOCATOR_OPEN1_add_missing_column_0 AS in0

)

SELECT *

FROM INS_LOCATOR_OPEN_EXP
