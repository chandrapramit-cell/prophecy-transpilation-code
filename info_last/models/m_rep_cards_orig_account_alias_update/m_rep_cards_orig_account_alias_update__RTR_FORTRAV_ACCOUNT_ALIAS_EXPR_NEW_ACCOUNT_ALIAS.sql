{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_ACCOUNT_ALIAS_sp AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_account_alias_update', 'EXP_ACCOUNT_ALIAS_sp') }}

),

EXP_ACCOUNT_ALIAS_reformat AS (

  SELECT 
    APPLICATION_NUMBER AS APPLICATION_NUMBER,
    NEW_ACCID AS NEW_ACCID,
    EXISTING_ACCID AS EXISTING_ACCID,
    'U' AS SOURCE_SYSTEM_CODE,
    563 AS CLASS,
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
          THEN (ERROR('No Business Date found on dbattriute'))
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
  
  FROM EXP_ACCOUNT_ALIAS_sp AS in0

),

RTR_FORTRAV_ACCOUNT_ALIAS AS (

  SELECT * 
  
  FROM EXP_ACCOUNT_ALIAS_reformat AS in0
  
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
