{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH UnionByName_1 AS (

  {{ prophecy_basics.UnionByName([], [], 'allowMissingColumns') }}

)

SELECT *

FROM UnionByName_1
