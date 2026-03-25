{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH CountRecords_0 AS (

  {{ sf_test.CountRecords() }}

)

SELECT *

FROM CountRecords_0
