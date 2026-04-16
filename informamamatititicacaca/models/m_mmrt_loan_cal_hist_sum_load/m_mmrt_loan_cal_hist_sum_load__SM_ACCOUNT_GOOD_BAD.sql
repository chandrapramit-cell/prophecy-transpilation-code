{{
  config({    
    "materialized": "table",
    "alias": "SM_ACCOUNT_GOOD_BAD",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH EXP_CURRENT_DELINQUENCY AS (

  SELECT *
  
  FROM {{ ref('m_mmrt_loan_cal_hist_sum_load__EXP_CURRENT_DELINQUENCY')}}

),

SM_ACCOUNT_GOOD_BAD_EXP AS (

  SELECT 
    ACCID AS ACCID,
    ACCOUNT_STATUS AS ACCOUNT_STATUS,
    GOOD_BAD_FLAG AS GOOD_BAD_FLAG,
    PERIODID AS PERIODID
  
  FROM EXP_CURRENT_DELINQUENCY AS in0

)

SELECT *

FROM SM_ACCOUNT_GOOD_BAD_EXP
