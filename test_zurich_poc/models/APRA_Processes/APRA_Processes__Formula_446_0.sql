{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Summarize_413 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Summarize_413')}}

),

Formula_446_0 AS (

  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS variableTimestamp,
    *
  
  FROM Summarize_413 AS in0

)

SELECT *

FROM Formula_446_0
