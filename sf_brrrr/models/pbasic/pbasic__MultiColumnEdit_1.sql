{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH MultiColumnEdit_1 AS (

  {{ sf_test.MultiColumnEdit() }}

)

SELECT *

FROM MultiColumnEdit_1
