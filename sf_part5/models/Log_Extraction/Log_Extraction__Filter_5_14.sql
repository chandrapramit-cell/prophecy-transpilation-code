{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DetourEnd_26_14 AS (

  SELECT *
  
  FROM {{ ref('Log_Extraction__DetourEnd_26_14')}}

),

Filter_5_14 AS (

  {{ prophecy_basics.ToDo('key not found: 14') }}

)

SELECT *

FROM Filter_5_14
