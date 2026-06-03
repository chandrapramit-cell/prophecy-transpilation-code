{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH p_202504_xlsx_Q_35 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final', 
      'p_202504_xlsx_Q_35'
    )
  }}

),

AlteryxSelect_42 AS (

  SELECT 
    CAST(Assignment AS INTEGER) AS loan_ls_nb,
    `Trans Amount` AS inpt_am,
    * EXCEPT (`Company Code`, 
    `Posting Dt`, 
    `Entry Dt`, 
    `GslashL Account`, 
    `ProfitslashCost Center`, 
    `Document Type`, 
    `Document Header Text`, 
    `User Name`, 
    `Posting Key`, 
    `Amount in Group CCY`, 
    `Value Date`, 
    `Line Item Text`, 
    `Document Number`, 
    `Data Source`, 
    `Info Field 1`, 
    `Info Field 2`, 
    `Info Field 3`, 
    `Info Field 4`, 
    `Info Field 5`, 
    `Refernce Field 1`, 
    `Refernce Field 2`, 
    `Entry Time`, 
    `Document Line Item`, 
    `TransCCY Code`, 
    `Trading Partner`, 
    `Center Node`, 
    `Fiscal Year`, 
    `Cross-Comp Doc No`, 
    `Clearing Doc No`, 
    `Rec__entry_doc__`, 
    `Exchange Rate`, 
    `Ext___Exchange_Rate`, 
    `DebitslashCredit`, 
    `Clearing Dt`, 
    `Info Field 6`, 
    `Refernce Field 3`, 
    `Cost Center`, 
    `GslashL Account2`, 
    `Customer`, 
    `Vendor`, 
    `Dt__of_Last_Doc_Chng`, 
    `Dt__of_Last_Doc_Updt`, 
    `Transaction Code`, 
    `Ref___Doc___No`, 
    `Reason for Reversal`, 
    `Reverse_Doc___No`, 
    `Rev___Doc_Fiscal_Year`, 
    `Group CCY Code`, 
    `Group_Currency_Exh___Rate`, 
    `Document Status`, 
    `Doc_Post___to_Prev_Period_Ind`, 
    `Translation Date`, 
    `Amount in Local CCY`, 
    `Local CCY Code`, 
    `Group CCY Exh Rate`, 
    `Exchange Rate Type`, 
    `Exc__Rt___Diff_for_3rd_Local_Cuur`, 
    `Exchange Rate Type2`, 
    `Doc___is_flagged_for_Rev___Indicator`, 
    `Planned Dt for Reverese Posting`, 
    `Source Id`, 
    `Batch Id`, 
    `Doc Date`, 
    `Posting Period`, 
    `Transaction Type`, 
    `Tax_Jur___Code`, 
    `Order No`, 
    `GLAS Company`, 
    `GLAS Account`, 
    `GLAS Cost Cntr`, 
    `GLAS Counterparty LE`, 
    `GLAS Counterparty BU`, 
    `Partner Profit Center`, 
    `Client ID`, 
    `Contract ID`, 
    `Assignment`, 
    `Trans Amount`)
  
  FROM p_202504_xlsx_Q_35 AS in0

),

Formula_43_0 AS (

  SELECT 
    CAST((DATE_FORMAT((DATE_ADD((DATE_TRUNC('month', CURRENT_TIMESTAMP)), CAST(-1 AS INTEGER))), 'yyyyMM')) AS string) AS run_yr_mo,
    *
  
  FROM AlteryxSelect_42 AS in0

),

Filter_44 AS (

  SELECT * 
  
  FROM Formula_43_0 AS in0
  
  WHERE (NOT(loan_ls_nb IS NULL))

),

AlteryxSelect_45 AS (

  SELECT 
    run_yr_mo AS run_yr_mo,
    CAST(loan_ls_nb AS string) AS loan_ls_nb,
    inpt_am AS inpt_am,
    * EXCEPT (`run_yr_mo`, `loan_ls_nb`, `inpt_am`)
  
  FROM Filter_44 AS in0

),

AlteryxSelect_37 AS (

  SELECT *
  
  FROM {{ ref('Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__AlteryxSelect_37')}}

),

AlteryxSelect_36 AS (

  SELECT *
  
  FROM {{ ref('Op_Lease_Tax_Billing_and_Tax_Income_Prophecy_Usecase_Final__AlteryxSelect_36')}}

),

Union_47_reformat_0 AS (

  SELECT 
    inpt_am AS inpt_am,
    CAST(loan_ls_nb AS string) AS loan_ls_nb
  
  FROM AlteryxSelect_36 AS in0

),

AlteryxSelect_41 AS (

  SELECT 
    CAST(run_yr_mo AS string) AS run_yr_mo,
    CAST(loan_ls_nb AS string) AS loan_ls_nb,
    inpt_am AS inpt_am,
    * EXCEPT (`GL`, `MF03_LEVEL2`, `run_yr_mo`, `loan_ls_nb`, `inpt_am`)
  
  FROM AlteryxSelect_37 AS in0

),

Union_47_reformat_1 AS (

  SELECT 
    inpt_am AS inpt_am,
    CAST(loan_ls_nb AS string) AS loan_ls_nb,
    run_yr_mo AS run_yr_mo
  
  FROM AlteryxSelect_41 AS in0

),

Union_47_reformat_2 AS (

  SELECT 
    inpt_am AS inpt_am,
    CAST(loan_ls_nb AS string) AS loan_ls_nb,
    run_yr_mo AS run_yr_mo
  
  FROM AlteryxSelect_45 AS in0

),

Union_47 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_47_reformat_0', 'Union_47_reformat_1', 'Union_47_reformat_2'], 
      [
        '[{"name": "inpt_am", "dataType": "Double"}, {"name": "loan_ls_nb", "dataType": "String"}]', 
        '[{"name": "inpt_am", "dataType": "Double"}, {"name": "loan_ls_nb", "dataType": "String"}, {"name": "run_yr_mo", "dataType": "String"}]', 
        '[{"name": "inpt_am", "dataType": "Double"}, {"name": "loan_ls_nb", "dataType": "String"}, {"name": "run_yr_mo", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_47
