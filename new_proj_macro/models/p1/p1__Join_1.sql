{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH anonymous_string_fields_missing_format AS (

  {#Loads seed dataset s23 to refresh the target dataset by overwriting existing records.#}
  SELECT * 
  
  FROM {{ ref('s23')}}

),

s23 AS (

  SELECT * 
  
  FROM {{ ref('s23')}}

),

Join_1 AS (

  SELECT * 
  
  FROM anonymous_string_fields_missing_format AS in0
  INNER JOIN s23 AS in1

)

SELECT *

FROM Join_1
