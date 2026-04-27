{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'DBATTRIBUTE') }}

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

SEQ_accid_EXPR_4 AS (

  SELECT 
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    ACCID AS EXISTING_ACCID,
    ((ROW_NUMBER() OVER (PARTITION BY 1)) - 1) AS NEW_ACCID
  
  FROM SQ_WORK_CARDS_ORIG_STAGING AS in0

),

EXP_ACCOUNT_ALIAS_LKP_LOCAL_1_LOOKUP_7 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_2,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.APPLICATION_NUMBER AS APPLICATION_NUMBER,
    in1.EXISTING_ACCID AS EXISTING_ACCID,
    in1.NEW_ACCID AS NEW_ACCID
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN SEQ_accid_EXPR_4 AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_ACCOUNT_ALIAS_LKP_LOCAL_1_VARS_0 AS (

  SELECT 
    LOOKUP_VARIABLE_2 AS business_date_string__find_business_date_lkp_1,
    'businessdate' AS lookup_string,
    *
  
  FROM EXP_ACCOUNT_ALIAS_LKP_LOCAL_1_LOOKUP_7 AS in0

),

EXP_ACCOUNT_ALIAS_LKP_LOCAL_1 AS (

  SELECT 
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    NEW_ACCID AS NEW_ACCID,
    EXISTING_ACCID AS EXISTING_ACCID
  
  FROM EXP_ACCOUNT_ALIAS_LKP_LOCAL_1_VARS_0 AS in0

),

EXP_ACCOUNT_ALIAS_JOIN AS (

  SELECT 
    in1.APPLICATION_NUMBER AS APPLICATION_NUMBER,
    in1.ACCID AS ACCID,
    in0.count1 AS count1,
    in0.business_date_string AS business_date_string,
    in0.business_date_string__find_business_date_lkp_1 AS business_date_string__find_business_date_lkp_1,
    in0.NEXTVAL AS NEXTVAL,
    in0.businessdate AS businessdate,
    in0.lookup_string AS lookup_string
  
  FROM EXP_ACCOUNT_ALIAS_LKP_LOCAL_1 AS in0
  INNER JOIN EXP_ACCOUNT_ALIAS_LKP_LOCAL_1 AS in1
     ON (
      (
        (
          ((in0.count1 = in1.count1) AND (in0.business_date_string = in1.business_date_string))
          AND (in0.business_date_string__find_business_date_lkp_1 = in1.business_date_string__find_business_date_lkp_1)
        )
        AND (in0.businessdate = in1.businessdate)
      )
      AND (in0.lookup_string = in1.lookup_string)
    )

),

EXP_ACCOUNT_ALIAS AS (

  SELECT 
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    NEXTVAL AS NEW_ACCID,
    ACCID AS EXISTING_ACCID,
    'U' AS SOURCE_SYSTEM_CODE,
    563 AS CLASS,
    businessdate AS business_date,
    (
      CASE
        WHEN (count1 = 1)
          THEN CAST((FEED_NUMBER_GENERATOR1('U', '360', business_date_string, SUBSTRING(business_date_string, 0, 8))) AS STRING)
        ELSE NULL
      END
    ) AS feed_update_id
  
  FROM EXP_ACCOUNT_ALIAS_JOIN AS in0

),

RTR_FORTRAV_ACCOUNT_ALIAS AS (

  SELECT * 
  
  FROM EXP_ACCOUNT_ALIAS AS in0
  
  WHERE ((NEW_ACCID IS NOT NULL) AND (EXISTING_ACCID IS NULL))

),

RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS AS (

  SELECT 
    APPLICATION_NUMBER AS APPLICATION_NUMBER1,
    NEW_ACCID AS NEW_ACCID1,
    EXISTING_ACCID AS EXISTING_ACCID1,
    SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE1,
    CLASS AS CLASS1,
    business_date AS business_date1,
    feed_update_id AS feed_update_id1,
    NEW_ACCID AS ACCID,
    SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE,
    APPLICATION_NUMBER AS ACCOUNT_ALIAS_ID,
    CLASS AS CLASS,
    business_date AS EFROM,
    feed_update_id AS FEED_UPDATE_ID
  
  FROM RTR_FORTRAV_ACCOUNT_ALIAS AS in0

)

SELECT *

FROM RTR_FORTRAV_ACCOUNT_ALIAS_EXPR_NEW_ACCOUNT_ALIAS
