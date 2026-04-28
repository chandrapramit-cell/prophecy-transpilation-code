{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CLIENT_ALIAS_sp AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_client_alias_update', 'EXP_CLIENT_ALIAS_sp') }}

),

EXP_CLIENT_ALIAS_reformat AS (

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
    (
      CASE
        WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1)
          THEN output_sp
        ELSE NULL
      END
    ) AS feed_update_id,
    lookup_string AS lookup_string,
    business_date_string__find_business_date_lkp_1 AS business_date_string__find_business_date_lkp_1,
    LOOKUP_VARIABLE_3 AS LOOKUP_VARIABLE_3,
    output_sp AS output_sp
  
  FROM EXP_CLIENT_ALIAS_sp AS in0

),

RTR_CLIENT_ALIAS AS (

  SELECT * 
  
  FROM EXP_CLIENT_ALIAS_reformat AS in0
  
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
