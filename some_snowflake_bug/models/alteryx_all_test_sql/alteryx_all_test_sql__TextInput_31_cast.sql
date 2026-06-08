{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "QA_TEST_RANDOM"
  })
}}

WITH TextInput_31 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_sql_31')}}

),

TextInput_31_cast AS (

  SELECT 
    "STORE NUMBER" AS "STORE NUMBER",
    "UNITS" AS "UNITS",
    CAST(UNITCOST AS FLOAT) AS UNITCOST
  
  FROM TextInput_31 AS in0

)

SELECT *

FROM TextInput_31_cast
