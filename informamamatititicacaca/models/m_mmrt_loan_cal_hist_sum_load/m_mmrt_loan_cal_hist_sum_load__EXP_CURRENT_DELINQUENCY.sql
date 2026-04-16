{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH `{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_loan_cal_hist_sum_load_SQTRANS` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'm_mmrt_loan_cal_hist_sum_load', 
      '{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_loan_cal_hist_sum_load_SQTRANS'
    )
  }}

),

EXP_CURRENT_DELINQUENCY AS (

  SELECT 
    STATUS AS STATUS,
    PAST_DUE AS PAST_DUE,
    (
      CASE
        WHEN (STATUS = '5')
          THEN 'Charged Off'
        WHEN (DELQ_210_DAYS > 0)
          THEN '210 days'
        WHEN (DELQ_180_DAYS > 0)
          THEN '180 days'
        WHEN (DELQ_150_DAYS > 0)
          THEN '150 days'
        WHEN (DELQ_120_DAYS > 0)
          THEN '120 days'
        WHEN (DELQ_90_DAYS > 0)
          THEN '90 days'
        WHEN (DELQ_60_DAYS > 0)
          THEN '60 days'
        WHEN (DELQ_30_DAYS > 0)
          THEN '30 days'
        WHEN (PAST_DUE > 0)
          THEN 'Past Due'
        ELSE 'Current'
      END
    ) AS CURRENT_DEINQUENCY,
    PERIODID AS PERIODID,
    ACC_NUM AS ACC_NUM,
    ARREARS_VALUE AS ARREARS_VALUE,
    BLOCK_CODE AS BLOCK_CODE,
    CHARGE_OFF_AMOUNT AS CHARGE_OFF_AMOUNT,
    CURR_BALANCE AS CURR_BALANCE,
    CYCLE_DAY AS CYCLE_DAY,
    GOOD_BAD_FLAG AS GOOD_BAD_FLAG,
    PAYMENT_METHOD AS PAYMENT_METHOD,
    SCORE AS SCORE,
    ACCID AS ACCID,
    UNEARNED_INTEREST AS UNEARNED_INTEREST,
    REBATE_PROMOTION_INDICATOR AS REBATE_PROMOTION_INDICATOR,
    USER_CODE_1 AS USER_CODE_1,
    USER_CODE_2 AS USER_CODE_2,
    STATUS AS ACCOUNT_STATUS
  
  FROM `{11_0_1}_Database___Generate_Monthly_Financialsm_mmrt_loan_cal_hist_sum_load_SQTRANS` AS in0

)

SELECT *

FROM EXP_CURRENT_DELINQUENCY
