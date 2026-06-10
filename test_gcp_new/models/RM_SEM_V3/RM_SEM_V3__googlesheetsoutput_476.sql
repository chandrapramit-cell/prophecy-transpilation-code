{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH AlteryxSelect_338 AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__AlteryxSelect_338')}}

),

googlesheetsoutput_476 AS (

  {{ prophecy_basics.ToDo('Component type: google-sheets-output is not supported.') }}

)

SELECT *

FROM googlesheetsoutput_476
