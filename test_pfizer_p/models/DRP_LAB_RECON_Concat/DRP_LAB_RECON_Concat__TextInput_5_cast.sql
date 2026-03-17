{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_5 AS (

  SELECT * 
  
  FROM {{ ref('seed_5')}}

),

TextInput_5_cast AS (

  SELECT 
    CAST(C1071007 AS string) AS C1071007,
    CAST(C1071032 AS string) AS C1071032
  
  FROM TextInput_5 AS in0

)

SELECT *

FROM TextInput_5_cast
