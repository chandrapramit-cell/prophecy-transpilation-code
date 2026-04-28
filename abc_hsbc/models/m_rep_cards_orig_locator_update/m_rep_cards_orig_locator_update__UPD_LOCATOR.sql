{{
  config({    
    "materialized": "table",
    "alias": "UPD_LOCATOR",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH RTR_CARDS_ORIG_LOCATOR_EXPR_OPEN_CLOSE_LOCATOR AS (

  SELECT *
  
  FROM {{ ref('m_rep_cards_orig_locator_update__RTR_CARDS_ORIG_LOCATOR_EXPR_OPEN_CLOSE_LOCATOR')}}

),

UPD_LOCATOR1_add_missing_column_0 AS (

  SELECT 
    CAST(NULL AS NUMERIC) AS MERGE_CLID,
    *
  
  FROM RTR_CARDS_ORIG_LOCATOR_EXPR_OPEN_CLOSE_LOCATOR AS in0

),

UPD_LOCATOR_EXP AS (

  SELECT 
    CLID_LOCATOR3 AS CLID,
    SOURCE_TYPE3 AS SOURCE_TYPE,
    MERGE_CLID AS MERGE_CLID,
    EFROM_LOCATOR3 AS EFROM,
    CLASS_LOCATOR3 AS CLASS,
    SUBCLASS_LOCATOR3 AS SUBCLASS,
    VALUE_LOCATOR3 AS `VALUE`,
    CAST(NULL AS STRING) AS CONTACT_POINT_EXTRA_INFO,
    business_date_minus_13 AS ETO
  
  FROM UPD_LOCATOR1_add_missing_column_0 AS in0

)

SELECT *

FROM UPD_LOCATOR_EXP
