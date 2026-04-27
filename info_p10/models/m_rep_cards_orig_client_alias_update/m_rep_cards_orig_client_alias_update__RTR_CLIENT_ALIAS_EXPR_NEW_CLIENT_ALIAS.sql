{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ ref('sf')}}

),

EXP_CLIENT_ALIAS_SP_2 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_client_alias_update', 'EXP_CLIENT_ALIAS_SP_2') }}

),

EXP_CLIENT_ALIAS_SP_2_EXPR_8 AS (

  SELECT 
    NEXTVAL AS GENERATED_CLID,
    CLID AS CLID,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    NEXTVAL AS ADID,
    business_date_string AS business_date_string,
    business_date_string__find_business_date_lkp_1 AS business_date_string__find_business_date_lkp_1,
    businessdate AS businessdate,
    count1 AS count1,
    lookup_string AS lookup_string,
    feed_update_nbr__feed_number_generator1_sp_2 AS feed_update_nbr__feed_number_generator1_sp_2,
    feed_update_nbr__feed_number_generator1_sp_2__arg_1 AS feed_update_nbr__feed_number_generator1_sp_2__arg_1,
    feed_update_nbr__feed_number_generator1_sp_2__arg_2 AS feed_update_nbr__feed_number_generator1_sp_2__arg_2,
    feed_update_nbr__feed_number_generator1_sp_2__arg_3 AS feed_update_nbr__feed_number_generator1_sp_2__arg_3,
    feed_update_nbr__feed_number_generator1_sp_2__arg_4 AS feed_update_nbr__feed_number_generator1_sp_2__arg_4
  
  FROM EXP_CLIENT_ALIAS_SP_2 AS in0

),

EXP_CLIENT_ALIAS_LOOKUP_11 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_4,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.GENERATED_CLID AS GENERATED_CLID,
    in1.CLID AS CLID,
    in1.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    in1.ADID AS ADID,
    in1.business_date_string AS business_date_string,
    in1.business_date_string__find_business_date_lkp_1 AS business_date_string__find_business_date_lkp_1,
    in1.businessdate AS businessdate,
    in1.count1 AS count1,
    in1.lookup_string AS lookup_string,
    in1.feed_update_nbr__feed_number_generator1_sp_2 AS feed_update_nbr__feed_number_generator1_sp_2
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN EXP_CLIENT_ALIAS_SP_2_EXPR_8 AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_CLIENT_ALIAS_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_4 AS business_date_string__find_business_date_lkp_1,
    * EXCEPT (`business_date_string__find_business_date_lkp_1`, `lookup_string`)
  
  FROM EXP_CLIENT_ALIAS_LOOKUP_11 AS in0

),

EXP_CLIENT_ALIAS AS (

  SELECT 
    GENERATED_CLID AS GENERATED_CLID,
    CLID AS CLID,
    ADID AS ADID,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    (LTRIM((RTRIM(CUSTOMER_NUMBER)))) AS CUSTOMER_NUMBER_SHORT,
    0 AS SOURCE_TYPE,
    'U' AS SOURCE_SYSTEM_CODE,
    585 AS CLIENT_ALIAS_CLASS,
    450 AS RESIDES_CLASS,
    (
      CASE
        WHEN CAST((
          (
            CASE
              WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
              ELSE CAST(NULL AS STRING)
            END
          ) IS NULL
        ) AS BOOL)
          THEN (ERROR('No Business Date found on dbattribute'))
        ELSE (
          PARSE_TIMESTAMP(
            '%Y%m%d', 
            CAST(SUBSTRING(
              (
                CASE
                  WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 NULLS FIRST)) = 1)
                    THEN BUSINESS_DATE_STRING__FIND_BUSINESS_DATE_LKP_1
                  ELSE CAST(NULL AS STRING)
                END
              ), 
              0, 
              8) AS STRING))
        )
      END
    ) AS business_date,
    CAST(NULL AS STRING) AS feed_update_id
  
  FROM EXP_CLIENT_ALIAS_VARS_0 AS in0

),

RTR_CLIENT_ALIAS AS (

  SELECT * 
  
  FROM EXP_CLIENT_ALIAS AS in0
  
  WHERE (CLID IS NULL)

),

RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS AS (

  SELECT 
    GENERATED_CLID AS GENERATED_CLID1,
    CLID AS CLID1,
    ADID AS ADID1,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER1,
    CUSTOMER_NUMBER_SHORT AS CUSTOMER_NUMBER_SHORT1,
    SOURCE_TYPE AS SOURCE_TYPE1,
    SOURCE_SYSTEM_CODE AS SOURCE_SYSTEM_CODE1,
    CLIENT_ALIAS_CLASS AS CLIENT_ALIAS_CLASS1,
    RESIDES_CLASS AS RESIDES_CLASS1,
    business_date AS business_date1,
    feed_update_id AS feed_update_id1,
    GENERATED_CLID AS CLID,
    business_date AS CLIENT_HISTORY_EFFECT_FROM,
    feed_update_id AS FEED_UPDATE_ID
  
  FROM RTR_CLIENT_ALIAS AS in0

)

SELECT *

FROM RTR_CLIENT_ALIAS_EXPR_NEW_CLIENT_ALIAS
