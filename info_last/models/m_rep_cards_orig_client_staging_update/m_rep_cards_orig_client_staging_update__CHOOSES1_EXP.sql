{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH CHOOSES1_EXP AS (

  SELECT 
    CAST(NULL AS NUMERIC) AS CLID,
    CAST(NULL AS STRING) AS CLIENT_HISTORY_EFFECT_FROM,
    CAST(NULL AS NUMERIC) AS FEED_UPDATE_ID
  
  FROM `` AS in0

)

SELECT *

FROM CHOOSES1_EXP
