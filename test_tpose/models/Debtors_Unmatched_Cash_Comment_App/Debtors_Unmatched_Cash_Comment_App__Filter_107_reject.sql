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

Filter_107_reject AS (

  SELECT * 
  
  FROM aka_SnowflakePR_13 AS in0
  
  WHERE (
          (NOT((KEY_9AX = 'PLACEHOLDER') AND (USER_TYPE = 'RI')))
          OR (((KEY_9AX = 'PLACEHOLDER') AND (USER_TYPE = 'RI')) IS NULL)
        )

)

SELECT *

FROM Filter_107_reject
