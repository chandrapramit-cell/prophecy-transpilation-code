{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Sample_1 AS (

  {{ sf_test.Sample() }}

)

SELECT *

FROM Sample_1
