{{
  config({    
    "materialized": "table",
    "alias": "CLIENT_ALIAS1",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS')}}

),

INS_CLIENT_ALIAS_add_missing_column_0 AS (

  SELECT 
    CAST(NULL AS NUMERIC) AS MERGE_CLID,
    CAST(NULL AS TIMESTAMP) AS ETO,
    *
  
  FROM RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS AS in0

),

CLIENT_ALIAS1_EXP AS (

  SELECT 
    GENERATED_CLID1 AS CLID,
    SOURCE_TYPE1 AS SOURCE_TYPE,
    CAST(NULL AS NUMERIC) AS MERGE_CLID,
    SOURCE_SYSTEM_CODE1 AS SOURCE_SYSTEM_CODE,
    CUSTOMER_NUMBER_SHORT1 AS SOURCE_CLIENT_REF,
    business_date1 AS EFROM,
    ETO AS ETO,
    CLIENT_ALIAS_CLASS1 AS CLASS
  
  FROM INS_CLIENT_ALIAS_add_missing_column_0 AS in0

)

SELECT *

FROM CLIENT_ALIAS1_EXP
