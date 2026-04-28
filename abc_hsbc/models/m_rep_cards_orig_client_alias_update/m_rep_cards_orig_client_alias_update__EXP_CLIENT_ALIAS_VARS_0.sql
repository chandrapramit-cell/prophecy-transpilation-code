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

SQ_CLIENT_ALIAS AS (

  SELECT 
    /*+ parallel(C) parallel(P) use_hash(C P)*/
    DISTINCT C.CLID,
    P.CUSTOMER_NUMBER
  
  FROM CLIENT_ALIAS AS C, WORK_CARDS_ORIG_STAGING AS P
  
  WHERE ltrim(rtrim(C.SOURCE_CLIENT_REF (+))) = ltrim(rtrim(P.CUSTOMER_NUMBER))
        AND C.ETO (+) IS NULL
        AND C.SOURCE_TYPE (+) = 0
        AND C.SOURCE_SYSTEM_CODE (+) = 'U'
        AND C.CLASS (+) = '585'
        AND P.CUSTOMER_NUMBER IS NOT NULL

),

Shortcut_to_SEQ_clid_EXPR_60 AS (

  SELECT 
    CLID AS CLID,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    (((ROW_NUMBER() OVER (PARTITION BY 1)) - 1) + 1) AS GENERATED_CLID
  
  FROM SQ_CLIENT_ALIAS AS in0

),

Shortcut_to_SEQ_adid_EXPR_61 AS (

  SELECT 
    CLID AS CLID,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    GENERATED_CLID AS GENERATED_CLID,
    (((ROW_NUMBER() OVER (PARTITION BY 1)) - 1) + 1) AS ADID
  
  FROM Shortcut_to_SEQ_clid_EXPR_60 AS in0

),

EXP_CLIENT_ALIAS_LOOKUP_63 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_3,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.CLID AS CLID,
    in1.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    in1.GENERATED_CLID AS GENERATED_CLID,
    in1.ADID AS ADID
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN Shortcut_to_SEQ_adid_EXPR_61 AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_CLIENT_ALIAS_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_3 AS business_date_string__find_business_date_lkp_1,
    *
  
  FROM EXP_CLIENT_ALIAS_LOOKUP_63 AS in0

)

SELECT *

FROM EXP_CLIENT_ALIAS_VARS_0
