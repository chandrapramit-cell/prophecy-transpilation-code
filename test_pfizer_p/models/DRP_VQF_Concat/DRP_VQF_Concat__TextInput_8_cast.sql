{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_8 AS (

  SELECT * 
  
  FROM {{ ref('seed_8')}}

),

TextInput_8_cast AS (

  SELECT 
    CAST(C1071007 AS string) AS C1071007,
    CAST(c10071032 AS string) AS c10071032,
    CAST(old_field AS string) AS old_field
  
  FROM TextInput_8 AS in0

)

SELECT *

FROM TextInput_8_cast
