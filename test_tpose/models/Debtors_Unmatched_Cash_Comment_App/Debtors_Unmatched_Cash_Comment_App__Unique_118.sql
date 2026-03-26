{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH aka_SnowflakePR_13 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Debtors_Unmatched_Cash_Comment_App', 'aka_SnowflakePR_13') }}

),

Filter_111 AS (

  SELECT * 
  
  FROM aka_SnowflakePR_13 AS in0
  
  WHERE (KEY_9AX = 'PLACEHOLDER')

),

Unique_118 AS (

  SELECT * 
  
  FROM Filter_111 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY KEY_9AX ORDER BY KEY_9AX) = 1

)

SELECT *

FROM Unique_118
