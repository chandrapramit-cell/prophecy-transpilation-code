{{
  config({    
    "materialized": "table",
    "alias": "REP_INF_WORKFLOW",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH `{11_0_1}_Database___Generate_Monthly_Financialsm_rep_sunday_check_SQ_DBATTRIBUTE` AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'm_rep_sunday_check', 
      '{11_0_1}_Database___Generate_Monthly_Financialsm_rep_sunday_check_SQ_DBATTRIBUTE'
    )
  }}

),

EXPTRANS AS (

  SELECT '1' AS DECISION
  
  FROM `{11_0_1}_Database___Generate_Monthly_Financialsm_rep_sunday_check_SQ_DBATTRIBUTE` AS in0

)

SELECT *

FROM EXPTRANS
