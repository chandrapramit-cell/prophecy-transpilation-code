{{
  config({    
    "materialized": "table",
    "alias": "CLIENT_FLAG",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CLIENT_FLAG_reformat AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_client_flag_update__EXP_CLIENT_FLAG_reformat')}}

),

RTR_CLIENT_FLAG_out0 AS (

  SELECT * 
  
  FROM EXP_CLIENT_FLAG_reformat AS in0
  
  WHERE (
          (APPLICATION_STATUS = 'DECLINED')
          AND (CAST((DATE_ADD(CAST(business_date AS DATE), INTERVAL -6 MONTH)) AS TIMESTAMP) < LAST_ACTION_DATE)
        )

),

CLIENT_FLAG_EXP AS (

  SELECT 
    CLID AS CLID,
    SOURCE_TYPE AS SOURCE_TYPE,
    MERGE_CLID AS MERGE_CLID,
    CREDIT_DECLINES_CLASS AS CLASS,
    CREDIT_DECLINES_SUBCLASS AS SUBCLASS,
    business_date AS EFROM,
    CAST(NULL AS STRING) AS ETO
  
  FROM RTR_CLIENT_FLAG_out0 AS in0

)

SELECT *

FROM CLIENT_FLAG_EXP
