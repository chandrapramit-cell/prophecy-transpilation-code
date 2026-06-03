{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_37 AS (

  SELECT *
  
  FROM {{ ref('Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__AlteryxSelect_37')}}

),

Formula_38_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (GL = 'a')
          THEN run_yr_mo
        ELSE 0
      END
    ) AS DOUBLE) AS run_yr_mo,
    * EXCEPT (`run_yr_mo`)
  
  FROM AlteryxSelect_37 AS in0

),

Filter_39 AS (

  SELECT * 
  
  FROM Formula_38_0 AS in0
  
  WHERE (
          NOT(
            run_yr_mo = 0)
        )

),

AlteryxSelect_40 AS (

  SELECT 
    CAST(run_yr_mo AS string) AS run_yr_mo,
    CAST(loan_ls_nb AS string) AS loan_ls_nb,
    inpt_am AS inpt_am,
    * EXCEPT (`GL`, `MF03_LEVEL2`, `run_yr_mo`, `loan_ls_nb`, `inpt_am`)
  
  FROM Filter_39 AS in0

),

AlteryxSelect_36 AS (

  SELECT *
  
  FROM {{ ref('Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__AlteryxSelect_36')}}

),

Union_46_reformat_0 AS (

  SELECT 
    inpt_am AS inpt_am,
    CAST(loan_ls_nb AS string) AS loan_ls_nb
  
  FROM AlteryxSelect_36 AS in0

),

Union_46_reformat_1 AS (

  SELECT 
    inpt_am AS inpt_am,
    CAST(loan_ls_nb AS string) AS loan_ls_nb,
    run_yr_mo AS run_yr_mo
  
  FROM AlteryxSelect_40 AS in0

),

Union_46 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_46_reformat_0', 'Union_46_reformat_1'], 
      [
        '[{"name": "inpt_am", "dataType": "Double"}, {"name": "loan_ls_nb", "dataType": "String"}]', 
        '[{"name": "inpt_am", "dataType": "Double"}, {"name": "loan_ls_nb", "dataType": "String"}, {"name": "run_yr_mo", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_46
