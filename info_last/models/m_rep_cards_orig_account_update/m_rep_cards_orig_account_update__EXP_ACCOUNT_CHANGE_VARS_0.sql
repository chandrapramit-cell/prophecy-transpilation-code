{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_ACCOUNT_COMPARE AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_account_update__EXP_ACCOUNT_COMPARE')}}

),

DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ ref('s1')}}

),

EXP_ACCOUNT_COMPARE_EXPR_44 AS (

  SELECT 
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    o_comp_acc_type AS o_comp_acc_type1,
    o_comp_source_code AS o_comp_source_code1,
    o_comp_store_number AS o_comp_store_number1,
    o_compare_status AS o_compare_status1,
    o_compare_status_date AS o_compare_status_date1,
    DATE_LAST_ACC_STATUS_CHANGE_TGT AS DATE_LAST_ACC_STATUS_CHANGE_TGT1,
    prophecy_sk AS prophecy_sk,
    ACCID_ACCOUNT_ALIAS AS ACCID_ACCOUNT_ALIAS,
    ACCID_ACCOUNT AS ACCID_ACCOUNT,
    CLID_CLIENT_ALIAS AS CLID_CLIENT_ALIAS,
    MERGE_CLID AS MERGE_CLID,
    o_PRODUCT_SRC AS o_PRODUCT_SRC,
    APPLICATION_SOURCE_CODE AS APPLICATION_SOURCE_CODE,
    APPLICATION_STORE_NUMBER AS APPLICATION_STORE_NUMBER,
    o_APPLICATION_STATUS_SRC AS o_APPLICATION_STATUS_SRC,
    LAST_ACTION_DATE_SRC AS LAST_ACTION_DATE_SRC,
    ACC_TYPE_TGT AS ACC_TYPE_TGT,
    SOURCE_CODE_TGT AS SOURCE_CODE_TGT,
    DOMICILE_BRANCH_TGT AS DOMICILE_BRANCH_TGT,
    STATUS_TGT AS STATUS_TGT,
    STATUS_DATE_TGT AS STATUS_DATE_TGT
  
  FROM EXP_ACCOUNT_COMPARE AS in0

),

EXP_ACCOUNT_CHANGE_LOOKUP_52 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_3,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.APPLICATION_NUMBER AS APPLICATION_NUMBER,
    in1.o_comp_acc_type1 AS o_comp_acc_type1,
    in1.o_comp_source_code1 AS o_comp_source_code1,
    in1.o_comp_store_number1 AS o_comp_store_number1,
    in1.o_compare_status1 AS o_compare_status1,
    in1.o_compare_status_date1 AS o_compare_status_date1,
    in1.DATE_LAST_ACC_STATUS_CHANGE_TGT1 AS DATE_LAST_ACC_STATUS_CHANGE_TGT1,
    in1.prophecy_sk AS prophecy_sk
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN EXP_ACCOUNT_COMPARE_EXPR_44 AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_ACCOUNT_CHANGE_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_3 AS business_date_string__find_business_date_lkp_1,
    *
  
  FROM EXP_ACCOUNT_CHANGE_LOOKUP_52 AS in0

)

SELECT *

FROM EXP_ACCOUNT_CHANGE_VARS_0
