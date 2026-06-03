{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH tax_bill_202504_33 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final', 
      'tax_bill_202504_33'
    )
  }}

),

AlteryxSelect_37 AS (

  SELECT 
    POSTING_MON AS run_yr_mo,
    CAST(MF03_REF_NBR AS INTEGER) AS loan_ls_nb,
    AMT AS inpt_am,
    * EXCEPT (`POSTING_MON`, `MF03_REF_NBR`, `AMT`)
  
  FROM tax_bill_202504_33 AS in0

)

SELECT *

FROM AlteryxSelect_37
