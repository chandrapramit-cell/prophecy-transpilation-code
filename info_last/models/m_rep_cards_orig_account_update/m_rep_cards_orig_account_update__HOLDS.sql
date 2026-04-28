{{
  config({    
    "materialized": "table",
    "alias": "HOLDS",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTR_ACCOUNT_JOIN_EXPR_42 AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_account_update__RTR_ACCOUNT_JOIN_EXPR_42')}}

),

RTR_ACCOUNT_out4 AS (

  SELECT * 
  
  FROM RTR_ACCOUNT_JOIN_EXPR_42 AS in0
  
  WHERE ((CLID_CLIENT_ALIAS IS NOT NULL) AND (ACCID_ACCOUNT IS NULL))

),

HOLDS_EXP AS (

  SELECT 
    CLID_CLIENT_ALIAS AS CLID,
    SOURCE_TYPE AS SOURCE_TYPE,
    MERGE_CLID AS MERGE_CLID,
    ACCID_ACCOUNT_ALIAS AS ACCID,
    HOLDS_CLASS AS CLASS,
    BUSINESS_DATE AS EFROM,
    CAST(NULL AS STRING) AS ETO
  
  FROM RTR_ACCOUNT_out4 AS in0

)

SELECT *

FROM HOLDS_EXP
