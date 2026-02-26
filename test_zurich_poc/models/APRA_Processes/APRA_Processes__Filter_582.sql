{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_579_0 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_579_0')}}

),

Filter_582 AS (

  SELECT * 
  
  FROM Formula_579_0 AS in0
  
  WHERE (`Sheet Names` = 'TZAC')

)

SELECT *

FROM Filter_582
