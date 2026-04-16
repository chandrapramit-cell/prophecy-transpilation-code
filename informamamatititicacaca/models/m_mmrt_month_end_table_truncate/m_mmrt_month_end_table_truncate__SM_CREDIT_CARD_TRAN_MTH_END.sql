{{
  config({    
    "materialized": "table",
    "alias": "SM_CREDIT_CARD_TRAN_MTH_END",
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

SM_CREDIT_CARD_TRAN_MTH_END_EXP AS (

  SELECT 
    CAST(NULL AS DECIMAL (12, 0)) AS CLIENTID,
    VARCHAR2_COL AS HOUSEID,
    CAST(NULL AS string) AS CC_ACCOUNT_NUMBER,
    CAST(NULL AS DECIMAL (12, 0)) AS CC_TRANSACTION_ID,
    CAST(NULL AS string) AS CC_CARD_NUMBER,
    CAST(NULL AS string) AS CC_EFFECTIVE_DATE,
    CAST(NULL AS string) AS CC_TYPE,
    CAST(NULL AS DECIMAL (3, 0)) AS CC_MONETARY_CODE,
    CAST(NULL AS DECIMAL (18, 0)) AS CC_BILLED_AMOUNT,
    CAST(NULL AS string) AS CC_POSTING_DATE,
    CAST(NULL AS string) AS CC_AUTHORISATION_CODE,
    CAST(NULL AS DECIMAL (5, 0)) AS CC_PLAN_NUMBER,
    CAST(NULL AS string) AS CC_TRAMS_TYPE,
    CAST(NULL AS DECIMAL (5, 0)) AS CC_MERCHANT_CATEGORY,
    CAST(NULL AS DECIMAL (1, 0)) AS CC_INCOMING_INDICATOR,
    CAST(NULL AS string) AS CC_CARD_ACCEPTOR_ID,
    CAST(NULL AS string) AS CC_COUNTRY_CODE,
    CAST(NULL AS string) AS CC_TRANSACTION_GROUPING,
    CAST(NULL AS string) AS CC_MS_SPEND_INDICATOR,
    CAST(NULL AS string) AS CC_RETAILER_DESCRIPTION,
    CAST(NULL AS string) AS CC_DCA_RECOVERY_METHOD
  
  FROM `{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_month_end_table_truncate_SQ_DUMMY` AS in0

)

SELECT *

FROM SM_CREDIT_CARD_TRAN_MTH_END_EXP
