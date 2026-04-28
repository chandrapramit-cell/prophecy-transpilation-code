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
    /*+ parallel(wrk) parallel(ca) parallel(cf)             use_hash(wrk ca cf)*/
    DISTINCT ca.clid,
    ca.merge_clid,
    wrk.application_status,
    wrk.last_action_date
  
  FROM (
    SELECT 
      customer_number,
      application_status,
      max(last_action_date) AS last_action_date
    
    FROM work_cards_orig_staging AS wrk
    
    WHERE customer_number IS NOT NULL
    
    GROUP BY 
      customer_number, application_status
  ) AS wrk, client_alias AS ca, client_flag AS cf
  
  WHERE wrk.customer_number = ca.source_client_ref (+)
        and ca.clid = cf.clid (+)
        and ca.source_system_code = 'U'
        and ca.source_type = 0
        and ca.eto IS NULL
        and ca.class = '585'
        and cf.class (+) = '206'
        and cf.subclass (+) = '296'
        and cf.source_type (+) = 0
        and cf.eto (+) IS NULL
        and cf.clid IS NULL

),

EXP_CLIENT_FLAG_LOOKUP_73 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_3,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.CLID AS CLID,
    in1.MERGE_CLID AS MERGE_CLID,
    in1.APPLICATION_STATUS AS APPLICATION_STATUS,
    in1.LAST_ACTION_DATE AS LAST_ACTION_DATE
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN SQ_WORK_CARDS_ORIG_STAGING AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_CLIENT_FLAG_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_3 AS business_date_string__find_business_date_lkp_1,
    *
  
  FROM EXP_CLIENT_FLAG_LOOKUP_73 AS in0

)

SELECT *

FROM EXP_CLIENT_FLAG_VARS_0
