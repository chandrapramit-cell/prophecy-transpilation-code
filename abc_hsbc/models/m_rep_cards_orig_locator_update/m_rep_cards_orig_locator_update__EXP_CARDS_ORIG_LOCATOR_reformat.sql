{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH EXP_CARDS_ORIG_LOCATOR_sp AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('m_rep_cards_orig_locator_update', 'EXP_CARDS_ORIG_LOCATOR_sp') }}

),

EXP_CARDS_ORIG_LOCATOR_reformat AS (

  SELECT 
    CLID_CLIENT_ALIAS AS CLID_CLIENT_ALIAS,
    MERGE_CLID_CLIENT_ALIAS AS MERGE_CLID_CLIENT_ALIAS,
    CLASS AS CLASS,
    SUBCLASS AS SUBCLASS,
    VALUE AS `VALUE`,
    CLID_LOCATOR AS CLID_LOCATOR,
    EFROM_LOCATOR AS EFROM_LOCATOR,
    CLASS_LOCATOR AS CLASS_LOCATOR,
    SUBCLASS_LOCATOR AS SUBCLASS_LOCATOR,
    VALUE_LOCATOR AS VALUE_LOCATOR,
    CONTACT_POINT_EXTRA_INFO AS CONTACT_POINT_EXTRA_INFO,
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
      DATE_ADD(
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
        ), 
        INTERVAL -1 DAY)
    ) AS business_date_minus_1,
    (
      CASE
        WHEN ((SUM(1) OVER (PARTITION BY 1 ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) = 1)
          THEN output_sp
        ELSE NULL
      END
    ) AS feed_update_id,
    (
      CASE
        WHEN ((LTRIM((RTRIM(VALUE)))) = (LTRIM((RTRIM(VALUE_LOCATOR)))))
          THEN 0
        ELSE 1
      END
    ) AS TOTAL_COMPARE,
    lookup_string AS lookup_string,
    business_date_string__find_business_date_lkp_1 AS business_date_string__find_business_date_lkp_1,
    LOOKUP_VARIABLE_3 AS LOOKUP_VARIABLE_3,
    output_sp AS output_sp
  
  FROM EXP_CARDS_ORIG_LOCATOR_sp AS in0

)

SELECT *

FROM EXP_CARDS_ORIG_LOCATOR_reformat
