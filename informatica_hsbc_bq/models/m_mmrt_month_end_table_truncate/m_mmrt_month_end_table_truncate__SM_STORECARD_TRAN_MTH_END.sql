{{
  config({    
    "materialized": "table",
    "alias": "SM_STORECARD_TRAN_MTH_END",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SQ_DUMMY AS (

  SELECT *
  
  FROM {{ ref('m_mmrt_month_end_table_truncate__SQ_DUMMY')}}

),

SM_STORECARD_TRAN_MTH_END_EXP AS (

  SELECT 
    CAST(NULL AS NUMERIC) AS CLIENTID,
    VARCHAR2_COL AS SC_ACCOUNT_NUMBER,
    CAST(NULL AS STRING) AS SC_EFFECTIVE_DATE,
    CAST(NULL AS STRING) AS SC_POSTING_DATE,
    CAST(NULL AS NUMERIC) AS SC_TRANS_AMOUNT,
    CAST(NULL AS STRING) AS SC_TRANS_CODE,
    CAST(NULL AS NUMERIC) AS SC_TRANSACTION_ID
  
  FROM SQ_DUMMY AS in0

)

SELECT *

FROM SM_STORECARD_TRAN_MTH_END_EXP
