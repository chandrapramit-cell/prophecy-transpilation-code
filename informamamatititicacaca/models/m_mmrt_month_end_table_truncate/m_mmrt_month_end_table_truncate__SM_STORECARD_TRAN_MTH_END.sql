{{
  config({    
    "materialized": "table",
    "alias": "SM_STORECARD_TRAN_MTH_END",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH `{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_month_end_table_truncate_SQ_DUMMY` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'm_mmrt_month_end_table_truncate', 
      '{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_month_end_table_truncate_SQ_DUMMY'
    )
  }}

),

SM_STORECARD_TRAN_MTH_END_EXP AS (

  SELECT 
    CAST(NULL AS DECIMAL (12, 0)) AS CLIENTID,
    VARCHAR2_COL AS SC_ACCOUNT_NUMBER,
    CAST(NULL AS string) AS SC_EFFECTIVE_DATE,
    CAST(NULL AS string) AS SC_POSTING_DATE,
    CAST(NULL AS DECIMAL (17, 0)) AS SC_TRANS_AMOUNT,
    CAST(NULL AS string) AS SC_TRANS_CODE,
    CAST(NULL AS DECIMAL (12, 0)) AS SC_TRANSACTION_ID
  
  FROM `{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_month_end_table_truncate_SQ_DUMMY` AS in0

)

SELECT *

FROM SM_STORECARD_TRAN_MTH_END_EXP
