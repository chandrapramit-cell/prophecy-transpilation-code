{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH SQ_WORK_CARDS_ORIG_STAGING AS (

  SELECT *
  
  FROM (
    SELECT 
      a.customer_number,
      a.addressline1,
      a.addressline2,
      a.addressline3,
      a.addressline4,
      a.addressline5,
      a.addressline6,
      a.postcode,
      a.country,
      rank() OVER (PARTITION BY customer_number ORDER BY last_action_date DESC) r
    
    FROM work_cards_orig_staging AS a
    
    WHERE customer_number IS NOT NULL
  )
  
  WHERE r = 1

),

SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_0 AS (

  SELECT 
    (ROW_NUMBER() OVER ()) AS prophecy_sk,
    *
  
  FROM SQ_WORK_CARDS_ORIG_STAGING AS in0

)

SELECT *

FROM SQ_WORK_CARDS_ORIG_STAGING_GENERATE_SK_0
