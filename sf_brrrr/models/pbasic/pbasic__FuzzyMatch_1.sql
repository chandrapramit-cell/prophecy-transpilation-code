{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH FuzzyMatch_1 AS (

  {{ sf_test.FuzzyMatch() }}

)

SELECT *

FROM FuzzyMatch_1
