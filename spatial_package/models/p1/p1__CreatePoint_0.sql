{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH CreatePoint_0 AS (

  {{ prophecy_spatial.CreatePoint('', []) }}

)

SELECT *

FROM CreatePoint_0
