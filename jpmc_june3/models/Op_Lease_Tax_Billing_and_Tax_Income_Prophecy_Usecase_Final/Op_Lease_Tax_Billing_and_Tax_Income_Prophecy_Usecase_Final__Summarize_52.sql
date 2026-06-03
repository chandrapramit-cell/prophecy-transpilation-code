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

Summarize_48 AS (

  SELECT 
    SUM(inpt_am) AS INPT_AM,
    loan_ls_nb AS loan_ls_nb
  
  FROM Union_46 AS in0
  
  GROUP BY loan_ls_nb

),

Summarize_52 AS (

  SELECT SUM(INPT_AM) AS Sum_INPT_AM
  
  FROM Summarize_48 AS in0

)

SELECT *

FROM Summarize_52
