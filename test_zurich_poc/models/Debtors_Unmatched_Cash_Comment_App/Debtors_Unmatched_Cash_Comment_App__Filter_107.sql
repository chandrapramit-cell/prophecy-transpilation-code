{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH aka_SnowflakePR_13 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Debtors_Unmatched_Cash_Comment_App', 'aka_SnowflakePR_13') }}

),

Filter_107 AS (

  SELECT * 
  
  FROM aka_SnowflakePR_13 AS in0
  
  WHERE ((KEY_9AX = 'PLACEHOLDER') AND (USER_TYPE = 'RI'))

)

SELECT *

FROM Filter_107
