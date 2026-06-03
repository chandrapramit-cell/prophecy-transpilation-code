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

Summarize_49 AS (

  SELECT 
    SUM(inpt_am) AS Sum_inpt_am,
    loan_ls_nb AS loan_ls_nb
  
  FROM Union_47 AS in0
  
  GROUP BY loan_ls_nb

),

Summarize_53 AS (

  SELECT SUM(Sum_inpt_am) AS Sum_Sum_inpt_am
  
  FROM Summarize_49 AS in0

)

SELECT *

FROM Summarize_53
