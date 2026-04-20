{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_0 AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_cust_add_paf__SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_0')}}

),

SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_EXPR_383 AS (

  SELECT 
    ADDRESSLINE3 AS in3,
    ADDRESSLINE4 AS in4,
    ADDRESSLINE5 AS in5,
    ADDRESSLINE1 AS in2,
    ADDRESSLINE2 AS in1,
    POSTCODE AS in6,
    prophecy_sk AS prophecy_sk,
    ADDRESSLINE6 AS ADDRESSLINE6,
    COUNTRY AS COUNTRY,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER
  
  FROM SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_0 AS in0

)

SELECT *

FROM SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_EXPR_383
