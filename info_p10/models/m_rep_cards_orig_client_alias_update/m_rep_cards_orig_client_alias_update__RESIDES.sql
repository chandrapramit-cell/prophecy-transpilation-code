{{
  config({    
    "materialized": "table",
    "alias": "RESIDES",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_client_alias_update__RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS')}}

),

INS_RESIDES_add_missing_column_0 AS (

  SELECT 
    CAST(NULL AS NUMERIC) AS MERGE_CLID,
    CAST(NULL AS TIMESTAMP) AS ETO,
    *
  
  FROM RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS AS in0

),

RESIDES_EXP AS (

  SELECT 
    ADID1 AS ADID,
    GENERATED_CLID1 AS CLID,
    SOURCE_TYPE1 AS SOURCE_TYPE,
    CAST(NULL AS NUMERIC) AS MERGE_CLID,
    business_date1 AS EFROM,
    CAST(NULL AS STRING) AS ETO,
    RESIDES_CLASS1 AS CLASS
  
  FROM INS_RESIDES_add_missing_column_0 AS in0

)

SELECT *

FROM RESIDES_EXP
