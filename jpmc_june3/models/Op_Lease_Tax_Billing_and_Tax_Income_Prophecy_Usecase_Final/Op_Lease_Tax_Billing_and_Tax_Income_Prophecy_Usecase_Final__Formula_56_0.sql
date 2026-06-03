{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_46 AS (

  SELECT *
  
  FROM {{ ref('Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Union_46')}}

),

Formula_56_0 AS (

  SELECT 
    CAST((
      CONCAT(
        '.\\AFSL_INPT_REV_TAX_INCM_', 
        (DATE_FORMAT((DATE_ADD((DATE_TRUNC('month', CURRENT_TIMESTAMP)), CAST(-1 AS INTEGER))), 'yyyyMM')), 
        '.xlsx', 
        '|', 
        'Sheet1')
    ) AS string) AS TODAY,
    *
  
  FROM Union_46 AS in0

)

SELECT *

FROM Formula_56_0
