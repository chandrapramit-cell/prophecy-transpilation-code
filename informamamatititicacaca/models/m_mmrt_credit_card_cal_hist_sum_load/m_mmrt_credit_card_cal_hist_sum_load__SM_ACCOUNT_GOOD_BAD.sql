{{
  config({    
    "materialized": "table",
    "alias": "SM_ACCOUNT_GOOD_BAD",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH `{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_credit_card_cal_hist_sum_load_SQTRANS` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'm_mmrt_credit_card_cal_hist_sum_load', 
      '{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_credit_card_cal_hist_sum_load_SQTRANS'
    )
  }}

),

SQTRANS_EXPR_21 AS (

  SELECT 
    PERIODID AS PERIODID,
    ACCID AS ACCID,
    STATUS AS ACCOUNT_STATUS,
    GOOD_BAD_FLAG AS GOOD_BAD_FLAG
  
  FROM `{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_credit_card_cal_hist_sum_load_SQTRANS` AS in0

)

SELECT *

FROM SQTRANS_EXPR_21
