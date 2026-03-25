{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH GenerateRows_1 AS (

  {{ sf_test.GenerateRows() }}

)

SELECT *

FROM GenerateRows_1
