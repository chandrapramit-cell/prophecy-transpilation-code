{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_47 AS (

  SELECT *
  
  FROM {{ ref('Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__Union_47')}}

),

Formula_57_0 AS (

  SELECT 
    CAST((
      CONCAT(
        '.\\AFSL_INPT_EXPEN_TAX_BILL_', 
        (DATE_FORMAT((DATE_ADD((DATE_TRUNC('month', CURRENT_TIMESTAMP)), CAST(-1 AS INTEGER))), 'yyyyMM')), 
        '.xlsx', 
        '|', 
        'Sheet1')
    ) AS string) AS TODAY,
    *
  
  FROM Union_47 AS in0

)

SELECT *

FROM Formula_57_0
