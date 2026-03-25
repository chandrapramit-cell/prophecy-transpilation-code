{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DataCleansing_1 AS (

  {{ sf_test.DataCleansing() }}

)

SELECT *

FROM DataCleansing_1
