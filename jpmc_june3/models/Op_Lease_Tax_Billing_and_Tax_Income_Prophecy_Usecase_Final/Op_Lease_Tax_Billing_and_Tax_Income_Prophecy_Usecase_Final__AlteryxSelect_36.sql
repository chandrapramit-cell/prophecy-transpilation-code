{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH state_202504_xl_34 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final', 
      'state_202504_xl_34'
    )
  }}

),

AlteryxSelect_36 AS (

  SELECT 
    CAST(RUN_YR_MO AS string) AS RUN_YR_MO,
    LSE_NBR AS loan_ls_nb,
    INPT_AM AS inpt_am,
    * EXCEPT (`variableTYPE`, `RUN_YR_MO`, `inpt_am`, `LSE_NBR`)
  
  FROM state_202504_xl_34 AS in0

)

SELECT *

FROM AlteryxSelect_36
