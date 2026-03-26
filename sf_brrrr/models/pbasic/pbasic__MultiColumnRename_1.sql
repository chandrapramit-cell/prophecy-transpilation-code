{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH MultiColumnRename_1 AS (

  {{ sf_test.MultiColumnRename() }}

)

SELECT *

FROM MultiColumnRename_1
