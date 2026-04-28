{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CLIENT_FLAG_sp AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_client_flag_update', 'EXP_CLIENT_FLAG_sp') }}

),

EXP_CLIENT_FLAG_reformat AS (

  SELECT 
    CLID AS CLID,
    MERGE_CLID AS MERGE_CLID,
    APPLICATION_STATUS AS APPLICATION_STATUS,
    LAST_ACTION_DATE AS LAST_ACTION_DATE,
    206 AS CREDIT_DECLINES_CLASS,
    296 AS CREDIT_DECLINES_SUBCLASS,
    0 AS SOURCE_TYPE,
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
  
  FROM EXP_CLIENT_FLAG_sp AS in0

)

SELECT *

FROM EXP_CLIENT_FLAG_reformat
