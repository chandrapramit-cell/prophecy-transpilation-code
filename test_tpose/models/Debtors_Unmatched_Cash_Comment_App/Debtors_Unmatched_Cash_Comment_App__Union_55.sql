{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_55 AS (

  {{ prophecy_basics.ToDo('next on empty iterator') }}

)

SELECT *

FROM Union_55
