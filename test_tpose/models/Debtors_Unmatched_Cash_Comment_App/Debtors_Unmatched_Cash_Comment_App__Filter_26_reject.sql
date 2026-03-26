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

Filter_26_reject AS (

  SELECT * 
  
  FROM aka_SnowflakePR_13 AS in0
  
  WHERE (
          (NOT((KEY_9AX = 'PLACEHOLDER') AND (USER_TYPE = 'GI')))
          OR (((KEY_9AX = 'PLACEHOLDER') AND (USER_TYPE = 'GI')) IS NULL)
        )

)

SELECT *

FROM Filter_26_reject
