{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Source__User_Db_31 AS (

  SELECT 
    loan_nb,
    lob,
    bus_mo,
    ci_nci_cd,
    cc_nb,
    lob_desc,
    gl_acct_nb,
    acct_gen17_alias_nm,
    orgn_sys_cd,
    line_item,
    heritage,
    current_timestamp() AS Data_Refresh_Date,
    SUM(bal_am) AS bal_am
  
  FROM 90278_ctg_prod.fdi_refined_mb_srvc_datamart_schema.mb_own_loan_rpt
  
  WHERE lob IN ('ASSET MANAGEMENT', 'Real Estate Portfolios')
        AND bus_mo IN (
              date_format(date_sub(date_trunc('quarter', current_date()), 1), 'yyyyMM'),
              date_format(date_sub(add_months(date_trunc('quarter', current_date()), -3), 1), 'yyyyMM'),
              date_format(date_sub(add_months(date_trunc('quarter', current_date()), -6), 1), 'yyyyMM'),
              date_format(date_sub(add_months(date_trunc('quarter', current_date()), -9), 1), 'yyyyMM')
            )
        AND line_item LIKE 'Loans -%'
        AND bal_am != 0
  
  GROUP BY 
    loan_nb, lob, bus_mo, ci_nci_cd, cc_nb, lob_desc, gl_acct_nb, acct_gen17_alias_nm, orgn_sys_cd, line_item, heritage

),

AlteryxSelect_33 AS (

  SELECT 
    acct_gen17_alias_nm AS acct_gen17_alias_nm,
    bus_mo AS `Bus Mo`,
    cc_nb AS `Cost Center`,
    ci_nci_cd AS CIslashNCI,
    gl_acct_nb AS `GL Account Number`,
    heritage AS Heritage,
    loan_nb AS `Loan Number`,
    lob AS LOB,
    lob_desc AS `Reporting Group`,
    orgn_sys_cd AS `Source System`,
    line_item AS `Line Item`,
    bal_am AS `Bal Am`,
    data_refresh_date AS `Data Refresh Date`,
    * EXCEPT (`acct_gen17_alias_nm`, 
    `Heritage`, 
    `LOB`, 
    `bus_mo`, 
    `cc_nb`, 
    `ci_nci_cd`, 
    `gl_acct_nb`, 
    `loan_nb`, 
    `lob_desc`, 
    `orgn_sys_cd`, 
    `line_item`, 
    `bal_am`, 
    `data_refresh_date`)
  
  FROM Source__User_Db_31 AS in0

)

SELECT *

FROM AlteryxSelect_33
