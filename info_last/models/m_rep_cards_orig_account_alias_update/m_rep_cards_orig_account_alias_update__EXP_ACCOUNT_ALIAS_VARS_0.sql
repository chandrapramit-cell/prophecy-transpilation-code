{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ ref('s1')}}

),

SQ_WORK_CARDS_ORIG_STAGING AS (

  SELECT 
    /*+ parallel(wrk aa)*/
    DISTINCT wrk.application_number,
    aa.accid
  
  FROM work_cards_orig_staging AS wrk, account_alias AS aa
  
  WHERE wrk.application_number = aa.account_alias_id (+)
        and aa.eto (+) IS NULL
        and aa.source_system_code (+) = 'U'
        and aa.class (+) = '563'

),

SEQ_accid_EXPR_5 AS (

  SELECT 
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    ACCID AS EXISTING_ACCID,
    ((ROW_NUMBER() OVER (PARTITION BY 1)) - 1) AS NEW_ACCID
  
  FROM SQ_WORK_CARDS_ORIG_STAGING AS in0

),

EXP_ACCOUNT_ALIAS_LOOKUP_12 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_3,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.APPLICATION_NUMBER AS APPLICATION_NUMBER,
    in1.EXISTING_ACCID AS EXISTING_ACCID,
    in1.NEW_ACCID AS NEW_ACCID
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN SEQ_accid_EXPR_5 AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_ACCOUNT_ALIAS_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_3 AS business_date_string__find_business_date_lkp_1,
    *
  
  FROM EXP_ACCOUNT_ALIAS_LOOKUP_12 AS in0

)

SELECT *

FROM EXP_ACCOUNT_ALIAS_VARS_0
