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

SQ_LOCATOR AS (

  SELECT *
  
  FROM (
    SELECT 
      /*+ parallel(cus) parallel(ca) parallel(l) use_hash(cus ca l)*/
      ca.clid AS clid,
      ca.merge_clid AS merge_clid,
      402 AS class,
      417 AS subclass,
      ltrim(rtrim(cus.email)) AS value,
      l.clid AS lkp_clid,
      l.efrom AS lkp_efrom,
      l.class AS lkp_class,
      l.subclass AS lkp_subclass,
      l.value AS lkp_value,
      l.contact_point_extra_info AS lkp_contact_point_extra_info,
      row_number() OVER (PARTITION BY cus.customer_number ORDER BY last_action_date DESC) row_number
    
    FROM work_cards_orig_staging AS cus, client_alias AS ca, locator AS l
    
    WHERE cus.customer_number = ca.source_client_ref (+)
          and cus.customer_number IS NOT NULL
          and ca.source_type (+) = 0
          and ca.eto (+) IS NULL
          and l.clid (+) = ca.clid
          and l.eto (+) IS NULL
          and l.source_type (+) = 0
          and l.class (+) = 402
          and l.subclass (+) = 417
    
    UNION
    
    SELECT 
      /*+ parallel(cus) parallel(ca) parallel(l) use_hash(cus ca l)*/
      ca.clid AS clid,
      ca.merge_clid AS merge_clid,
      400 AS class,
      414 AS subclass,
      ltrim(rtrim(cus.telephone_number)) AS value,
      l.clid AS lkp_clid,
      l.efrom AS lkp_efrom,
      l.class AS lkp_class,
      l.subclass AS lkp_subclass,
      l.value AS lkp_value,
      l.contact_point_extra_info AS lkp_contact_point_extra_info,
      row_number() OVER (PARTITION BY cus.customer_number ORDER BY last_action_date DESC) row_number
    
    FROM work_cards_orig_staging AS cus, client_alias AS ca, locator AS l
    
    WHERE cus.customer_number = ca.source_client_ref (+)
          and cus.customer_number IS NOT NULL
          and ca.source_type (+) = 0
          and ca.eto (+) IS NULL
          and l.clid (+) = ca.clid
          and l.eto (+) IS NULL
          and l.source_type (+) = 0
          and l.class (+) = 400
          and l.subclass (+) = 414
  )
  
  WHERE row_number = 1

),

EXP_CARDS_ORIG_LOCATOR_LOOKUP_57 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_3,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.CLID_CLIENT_ALIAS AS CLID_CLIENT_ALIAS,
    in1.MERGE_CLID_CLIENT_ALIAS AS MERGE_CLID_CLIENT_ALIAS,
    in1.CLASS AS CLASS,
    in1.SUBCLASS AS SUBCLASS,
    in1.VALUE AS `VALUE`,
    in1.CLID_LOCATOR AS CLID_LOCATOR,
    in1.EFROM_LOCATOR AS EFROM_LOCATOR,
    in1.CLASS_LOCATOR AS CLASS_LOCATOR,
    in1.SUBCLASS_LOCATOR AS SUBCLASS_LOCATOR,
    in1.VALUE_LOCATOR AS VALUE_LOCATOR,
    in1.CONTACT_POINT_EXTRA_INFO AS CONTACT_POINT_EXTRA_INFO
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN SQ_LOCATOR AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_CARDS_ORIG_LOCATOR_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_3 AS business_date_string__find_business_date_lkp_1,
    *
  
  FROM EXP_CARDS_ORIG_LOCATOR_LOOKUP_57 AS in0

)

SELECT *

FROM EXP_CARDS_ORIG_LOCATOR_VARS_0
