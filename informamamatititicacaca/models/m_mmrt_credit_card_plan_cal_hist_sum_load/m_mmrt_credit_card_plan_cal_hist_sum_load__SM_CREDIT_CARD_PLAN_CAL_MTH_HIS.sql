{{
  config({    
    "materialized": "table",
    "alias": "SM_CREDIT_CARD_PLAN_CAL_MTH_HIS",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH `{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_credit_card_plan_cal_hist_sum_load_SQ_CREDIT_PLAN` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'm_mmrt_credit_card_plan_cal_hist_sum_load', 
      '{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_credit_card_plan_cal_hist_sum_load_SQ_CREDIT_PLAN'
    )
  }}

),

SQ_CREDIT_PLAN_EXPR_6 AS (

  SELECT 
    ACCOUNT_NUMBER AS CC_ACCOUNT_NUMBER,
    APR_1 AS CC_PLAN_APR,
    CURRENT_BALANCE AS CC_PLAN_CURRENT_BALANCE,
    PLAN_NUMBER AS CC_PLAN_NUMBER,
    PLAN_TYPE AS CC_PLAN_TYPE,
    PERIODID AS PERIODID
  
  FROM `{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_credit_card_plan_cal_hist_sum_load_SQ_CREDIT_PLAN` AS in0

)

SELECT *

FROM SQ_CREDIT_PLAN_EXPR_6
