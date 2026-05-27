{{
  config({    
    "materialized": "table",
    "alias": "5_Input19_macro_ip",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_3 AS (

  SELECT * 
  
  FROM {{ ref('seed_test_3')}}

),

TextInput_3_cast AS (

  SELECT 
    CAST(Country AS string) AS Country,
    CAST(Product AS string) AS Product,
    CAST(Units AS INTEGER) AS Units
  
  FROM TextInput_3 AS in0

)

SELECT *

FROM TextInput_3_cast
