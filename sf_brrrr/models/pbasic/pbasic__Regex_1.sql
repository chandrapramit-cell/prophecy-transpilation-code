{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Regex_1 AS (

  {{ sf_test.Regex() }}

)

SELECT *

FROM Regex_1
