{{
  config({    
    "materialized": "table",
    "alias": "aka_SnowflakePR_12",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_55 AS (

  SELECT *
  
  FROM {{ ref('Debtors_Unmatched_Cash_Comment_App__Union_55')}}

),

Formula_62_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(COMMENT_CATEGORY, '_', ' ')) AS STRING) AS COMMENT_CATEGORY,
    * EXCEPT (`comment_category`)
  
  FROM Union_55 AS in0

),

AlteryxSelect_109 AS (

  SELECT 
    Transaction_ID AS TRANSACTION_ID,
    COMMENT_CATEGORY AS COMMENT_CATEGORY,
    COMMENTS AS COMMENTS,
    CLIENT_ID AS CLIENT_ID,
    COMMENT_DATE AS COMMENT_DATE,
    CTL_REC_CRTN_DATE AS CTL_REC_CRTN_DATE,
    CTL_REC_UPDATE_DATE AS CTL_REC_UPDATE_DATE,
    CTL_SRC_SYS_SET_NAME AS CTL_SRC_SYS_SET_NAME,
    EMAIL AS EMAIL
  
  FROM Formula_62_0 AS in0

)

SELECT *

FROM AlteryxSelect_109
