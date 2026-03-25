{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH FindDuplicates_1 AS (

  {{ sf_test.FindDuplicates() }}

)

SELECT *

FROM FindDuplicates_1
