{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH DynamicSelect_1 AS (

  {{ sf_test.DynamicSelect() }}

)

SELECT *

FROM DynamicSelect_1
