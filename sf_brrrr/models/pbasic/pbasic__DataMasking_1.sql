{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DataMasking_1 AS (

  {{ sf_test.DataMasking() }}

)

SELECT *

FROM DataMasking_1
